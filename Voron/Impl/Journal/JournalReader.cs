﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Voron.Impl.Extensions;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public unsafe class JournalReader
	{
		private readonly IVirtualPager _pager;
		private readonly IVirtualPager _recoveryPager;

		private readonly long _lastSyncedTransactionId;
		private long _readingPage;

		private uint _previousTransactionCrc;

		private readonly Dictionary<long, JournalFile.PagePosition> _transactionPageTranslation = new Dictionary<long, JournalFile.PagePosition>();
		private int _recoveryPage;

		public bool RequireHeaderUpdate { get; private set; }

		public long NextWritePage
		{
			get { return _readingPage; }
		}

		public JournalReader(IVirtualPager pager, IVirtualPager recoveryPager, long lastSyncedTransactionId, TransactionHeader* previous)
		{
			if (pager == null) throw new ArgumentNullException("pager");

			RequireHeaderUpdate = false;
			_pager = pager;
			_recoveryPager = recoveryPager;
			_lastSyncedTransactionId = lastSyncedTransactionId;
			_readingPage = 0;
			_recoveryPage = 0;
			LastTransactionHeader = previous;
			_previousTransactionCrc = 0;
		}

		public TransactionHeader* LastTransactionHeader { get; private set; }

		protected bool ReadOneTransactionForShipping(StorageEnvironmentOptions options, out TransactionToShip transactionToShipRecord)
		{
			transactionToShipRecord = null;
			if (_readingPage >= _pager.NumberOfAllocatedPages)
				return false;

			TransactionHeader* current;
			if (!TryReadAndValidateHeader(options, out current))
				return false;

			var compressedPageCount = (current->CompressedSize / AbstractPager.PageSize) + (current->CompressedSize % AbstractPager.PageSize == 0 ? 0 : 1);
			if (current->TransactionId <= _lastSyncedTransactionId)
			{
				LastTransactionHeader = current;
				_readingPage += compressedPageCount;
				return true; // skipping
			}

			if (!ValidatePagesCrc(options, compressedPageCount, current))
				return false;

			var compressedPagesRaw = new byte[compressedPageCount * AbstractPager.PageSize];
			fixed (byte* compressedDataPtr = compressedPagesRaw)
				NativeMethods.memcpy(compressedDataPtr, _pager.AcquirePagePointer(_readingPage), compressedPageCount * AbstractPager.PageSize);

			transactionToShipRecord = new TransactionToShip(*current)
			{
				CompressedData = new MemoryStream(compressedPagesRaw), //no need to compress the pages --> after being written to Journal they are already compressed
				PreviousTransactionCrc = _previousTransactionCrc
			};

			_previousTransactionCrc = current->Crc;

			_readingPage += compressedPageCount;
			return true;
		}

		public IEnumerable<TransactionToShip> ReadJournalForShipping(StorageEnvironmentOptions options)
		{
			TransactionToShip transactionToShip;
			while (ReadOneTransactionForShipping(options, out transactionToShip))
				yield return transactionToShip;
		}

		public bool ReadOneTransaction(StorageEnvironmentOptions options, bool checkCrc = true)
		{
			if (_readingPage >= _pager.NumberOfAllocatedPages)
				return false;

			TransactionHeader* current;
			if (!TryReadAndValidateHeader(options, out current))
				return false;

			var compressedPages = (current->CompressedSize / AbstractPager.PageSize) + (current->CompressedSize % AbstractPager.PageSize == 0 ? 0 : 1);

			if (current->TransactionId <= _lastSyncedTransactionId)
			{
				LastTransactionHeader = current;
				_readingPage += compressedPages;
				return true; // skipping
			}

			if (checkCrc && !ValidatePagesCrc(options, compressedPages, current))
				return false;

			var totalPageCount = current->PageCount + current->OverflowPageCount;

			_recoveryPager.EnsureContinuous(null, _recoveryPage, totalPageCount + 1);
			var dataPage = _recoveryPager.AcquirePagePointer(_recoveryPage);

			NativeMethods.memset(dataPage, 0, totalPageCount * AbstractPager.PageSize);
			try
			{
				LZ4.Decode64(_pager.AcquirePagePointer(_readingPage), current->CompressedSize, dataPage, current->UncompressedSize, true);
			}
			catch (Exception e)
			{
				options.InvokeRecoveryError(this, "Could not de-compress, invalid data", e);
				RequireHeaderUpdate = true;

				return false;
			}

			var tempTransactionPageTranslaction = (*current).GetTransactionToPageTranslation(_recoveryPager, ref _recoveryPage);

			_readingPage += compressedPages;

			LastTransactionHeader = current;

			foreach (var pagePosition in tempTransactionPageTranslaction)
			{
				_transactionPageTranslation[pagePosition.Key] = pagePosition.Value;
			}

			return true;
		}

		

		private bool ValidatePagesCrc(StorageEnvironmentOptions options, int compressedPages, TransactionHeader* current)
		{
			uint crc = Crc.Value(_pager.AcquirePagePointer(_readingPage), 0, compressedPages * AbstractPager.PageSize);

			if (crc != current->Crc)
			{
				RequireHeaderUpdate = true;
				options.InvokeRecoveryError(this, "Invalid CRC signature for transaction " + current->TransactionId, null);

				return false;
			}
			return true;
		}

		public void RecoverAndValidate(StorageEnvironmentOptions options)
		{
			if (_recoveryPager == null) throw new InvalidOperationException("recoveryPager should not be null");

			while (ReadOneTransaction(options))
			{
			}
		}

		

		public Dictionary<long, JournalFile.PagePosition> TransactionPageTranslation
		{
			get { return _transactionPageTranslation; }
		}

		private bool TryReadAndValidateHeader(StorageEnvironmentOptions options, out TransactionHeader* current)
		{
			current = (TransactionHeader*)_pager.Read(_readingPage).Base;

			if (current->HeaderMarker != Constants.TransactionHeaderMarker)
			{
				// not a transaction page, 

				// if the header marker is zero, we are probably in the area at the end of the log file, and have no additional log records
				// to read from it. This can happen if the next transaction was too big to fit in the current log file. We stop reading
				// this log file and move to the next one. 

				RequireHeaderUpdate = current->HeaderMarker != 0;
				if (RequireHeaderUpdate)
				{
					options.InvokeRecoveryError(this,
						"Transaction " + current->TransactionId +
						" header marker was set to garbage value, file is probably corrupted", null);
				}

				return false;
			}

			ValidateHeader(current, LastTransactionHeader);

			if (current->TxMarker.HasFlag(TransactionMarker.Commit) == false)
			{
				// uncommitted transaction, probably
				RequireHeaderUpdate = true;
				options.InvokeRecoveryError(this,
						"Transaction " + current->TransactionId +
						" was not committed", null);
				return false;
			}

			_readingPage++;
			return true;
		}

		private void ValidateHeader(TransactionHeader* current, TransactionHeader* previous)
		{
			if (current->TransactionId < 0)
				throw new InvalidDataException("Transaction id cannot be less than 0 (Tx: " + current->TransactionId + " )");
			if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->LastPageNumber < 0)
				throw new InvalidDataException("Last page number after committed transaction must be greater than 0");
			if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->PageCount > 0 && current->Crc == 0)
				throw new InvalidDataException("Committed and not empty transaction checksum can't be equal to 0");
			if (current->Compressed)
			{
				if (current->CompressedSize <= 0)
					throw new InvalidDataException("Compression error in transaction.");
			}
			else
				throw new InvalidDataException("Uncompressed transactions are not supported.");

			if (previous == null)
				return;

			if (current->TransactionId != 1 &&
				// 1 is a first storage transaction which does not increment transaction counter after commit
				current->TransactionId - previous->TransactionId != 1)
				throw new InvalidDataException("Unexpected transaction id. Expected: " + (previous->TransactionId + 1) +
											   ", got:" + current->TransactionId);
		}

		public override string ToString()
		{
			return _pager.ToString();
		}
	}
}
