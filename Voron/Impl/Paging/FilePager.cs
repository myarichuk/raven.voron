﻿using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Paging
{
    public unsafe class FilePager : AbstractPager
    {
        private readonly FileStream _fileStream;
        private readonly FileInfo _fileInfo;
	    private SafeFileHandle _safeFileHandle;

	    public FilePager(string file)
        {
            _fileInfo = new FileInfo(file);

            var noData = _fileInfo.Exists == false || _fileInfo.Length == 0;

            _safeFileHandle = NativeFileMethods.CreateFile(file,
	            NativeFileAccess.GenericRead | NativeFileAccess.GenericWrite,
	            NativeFileShare.Read, IntPtr.Zero,
	            NativeFileCreationDisposition.OpenAlways,
	            NativeFileAttributes.Write_Through | NativeFileAttributes.NoBuffering ,
	            IntPtr.Zero);

            if (_safeFileHandle.IsInvalid)
            {
                throw new Win32Exception();
            }

            _fileStream = new FileStream(_safeFileHandle, FileAccess.ReadWrite);

            if (noData)
            {
                NumberOfAllocatedPages = 0;
            }
            else
            {
				NumberOfAllocatedPages = _fileInfo.Length / PageSize;
	            PagerState.Release();
                PagerState = CreateNewPagerState();
            }
        }

        public FileStream FileStream { get { return _fileStream; }}

        public override byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null)
        {
            return (pagerState ?? PagerState).MapBase + (pageNumber * PageSize);
        }

	    protected override unsafe string GetSourceName()
	    {
		    if (_fileInfo == null)
			    return "Unknown";
		    return "File: " + _fileInfo.Name;
	    }

	    public override Page GetWritable(long pageNumber)
        {
            throw new InvalidOperationException("File pager does not offer writing directly to a page");
        }

        public override void AllocateMorePages(Transaction tx, long newLength)
        {
            if (newLength < _fileStream.Length)
                throw new ArgumentException("Cannot set the legnth to less than the current length");

            if (newLength == _fileStream.Length)
                return;

            // need to allocate memory again
			NativeFileMethods.SetFileLength(_safeFileHandle, newLength);

			Debug.Assert(_fileStream.Length == newLength);

			var tmp = PagerState;

            PagerState newPager = CreateNewPagerState();

            if (tx != null) // we only pass null during startup, and we don't need it there
            {
                newPager.AddRef(); // one for the current transaction
                tx.AddPagerState(newPager);
            }

            PagerState = newPager;
			tmp.Release(); // when the last transaction using this is over, will dispose it
            NumberOfAllocatedPages = newLength / PageSize;
        }

        private PagerState CreateNewPagerState()
        {
            var mmf = MemoryMappedFile.CreateFromFile(_fileStream, Guid.NewGuid().ToString(), _fileStream.Length,
                                                      MemoryMappedFileAccess.Read, null, HandleInheritability.None, true);

            MemoryMappedViewAccessor accessor;
            try
            {
                accessor = mmf.CreateViewAccessor(0, _fileStream.Length, MemoryMappedFileAccess.Read);
            }
            catch (Exception)
            {
                mmf.Dispose();
                throw;
            }
            byte* p = null;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);

            var newPager = new PagerState(this)
                 {
                     Accessor = accessor,
					 Files = new[] { mmf },
                     MapBase = p
                 };
            newPager.AddRef(); // one for the pager
            return newPager;
        }

        public override void Sync()
        {
            _fileStream.Flush(true);
        }

        public override int Write(Page page, long? pageNumber)
        {
            var number = pageNumber ?? page.PageNumber;

            Debug.Assert(number <= NumberOfAllocatedPages);


            var toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

            return WriteDirect(page, number, toWrite);
        }

        public override string ToString()
        {
            return _fileInfo.Name;
        }

        public override int WriteDirect(Page start, long pagePosition, int pagesToWrite)
        {
            if (_fileInfo.Extension == ".voron" && pagePosition > 1)
            {
                
            }
            var position = pagePosition * PageSize;
            var toWrite = pagesToWrite * PageSize;

            var overlapped = new Overlapped()
            {
                OffsetLow = (int)(position & 0xffffffff),
                OffsetHigh = (int)(position >> 32),
            };

	        var nativeOverlapped = overlapped.Pack(null, null);
	        try
	        {
				var startWrite = start.Base;
				while (toWrite != 0)
				{
					int written;
					if (NativeFileMethods.WriteFile(_safeFileHandle, startWrite, toWrite, out written, nativeOverlapped) == false)
					{
						throw new Win32Exception();
					}
					toWrite -= written;
					startWrite += written;
				}

		        return toWrite;
	        }
	        finally
	        {
		        Overlapped.Unpack(nativeOverlapped);
				Overlapped.Free(nativeOverlapped);
	        }
        }

        public override void Dispose()
        {
            if (Disposed)
                return;
            base.Dispose();
      
            _fileStream.Dispose();

            if (DeleteOnClose)
                _fileInfo.Delete();
        }
    }
}
