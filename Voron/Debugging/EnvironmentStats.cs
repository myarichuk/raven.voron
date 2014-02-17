﻿namespace Voron.Debugging
{
    public class EnvironmentStats
    {
        public long FreePages;
        public long FreePagesOverhead;
        public long RootPages;
        public long UnallocatedPagesAtEndOfFile;
        public long UsedDataFileSizeInBytes;
        public long AllocatedDataFileSizeInBytes;
    }
}