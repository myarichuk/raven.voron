using Voron.Impl;
using Xunit;

namespace Voron.Tests.Bugs
{ 
	public class RequiredPrefixWithSeekBeforeAllKeys : StorageTest
	{
		[Fact]
		public void SeekBeforeAllKeys_with_required_prefix_should_return_true_if_relevant_nodes_exist()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "testTree");
				tx.Commit();
			}

			var wb = new WriteBatch();
			wb.Add("AA", StreamFor("Foo1"), "testTree");
			wb.Add("AB", StreamFor("Foo2"), "testTree");

			wb.Add("ACA", StreamFor("Foo3"), "testTree");
			wb.Add("ACB", StreamFor("Foo4"), "testTree");
			wb.Add("ACC", StreamFor("Foo5"), "testTree");

			wb.Add("ADA", StreamFor("Foo6"), "testTree");
			wb.Add("ADB", StreamFor("Foo7"), "testTree");

			Env.Writer.Write(wb);

			using(var snapshot = Env.CreateSnapshot())
			using (var iterator = snapshot.Iterate("testTree"))
			{
				iterator.RequiredPrefix = "AC";
				Assert.True(iterator.Seek(Slice.BeforeAllKeys));
			}
		}


		[Fact]
		public void SeekBeforeAllKeys_with_required_prefix_should_return_false_if_relevant_nodes_doesnt_exist()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "testTree");
				tx.Commit();
			}

			var wb = new WriteBatch();
			wb.Add("AA", StreamFor("Foo1"), "testTree");
			wb.Add("AB", StreamFor("Foo2"), "testTree");

			wb.Add("ADA", StreamFor("Foo6"), "testTree");
			wb.Add("ADB", StreamFor("Foo7"), "testTree");

			Env.Writer.Write(wb);

			using (var snapshot = Env.CreateSnapshot())
			using (var iterator = snapshot.Iterate("testTree"))
			{
				iterator.RequiredPrefix = "AC";
				Assert.False(iterator.Seek(Slice.BeforeAllKeys));
			}
		}
	}
}
