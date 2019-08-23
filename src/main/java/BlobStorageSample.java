import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TransferManager;
import com.microsoft.azure.storage.blob.models.BlockBlobCommitBlockListResponse;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.reactivex.Flowable;
import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Random;


public class BlobStorageSample {
    private static final int BIG_BLOCK_SIZE = 1024 * 1024 * 4;
    private static final int SMALL_BLOCK_SIZE = 1024 * 1024 * 1;
    private static final int REALLY_SMALL_BLOCK_SIZE = 1024 * 128;

    public static void main(String[] args) throws Exception {
        String accountName = args[0];
        String accountKey = args[1];

        byte[] backingByteBuffer = new byte[1024 * 1024 * 1024];
        Random r = new Random();
        r.nextBytes(backingByteBuffer);
        ByteBuffer bbuf = ByteBuffer.wrap(backingByteBuffer);
        Flowable<ByteBuffer> oneChunkFlowable =  Flowable.just(bbuf);

        // Create a BlockBlobURL object that wraps a blob's URL and a default pipeline.
        // Warmup
        URL u = new URL(String.format(Locale.ROOT, "https://%s.blob.core.windows.net/", accountName));
        ServiceURL s = new ServiceURL(u,
            StorageURL.createPipeline(new SharedKeyCredentials(accountName, accountKey), new PipelineOptions()));
        ContainerURL containerURL = s.createContainerURL("newcontainer");
        String filename = "BigFile.bin";
        BlockBlobURL blobURL = containerURL.createBlockBlobURL(filename);
        FlowableUtil.split(bbuf, backingByteBuffer.length);
        long start = System.currentTimeMillis();
        BlockBlobCommitBlockListResponse blockBlobCommitBlockListResponse =
            TransferManager.uploadFromNonReplayableFlowable(oneChunkFlowable, blobURL, BIG_BLOCK_SIZE, 16, null)
                .blockingGet();
        long end = System.currentTimeMillis();
        long duration = end - start;
        System.out.println("Warmup done in " + duration + "ms.");
        System.out.println("Status code: " + blockBlobCommitBlockListResponse.statusCode());

        // Big
        start = System.currentTimeMillis();
        blockBlobCommitBlockListResponse =
            TransferManager.uploadFromNonReplayableFlowable(oneChunkFlowable, blobURL, BIG_BLOCK_SIZE, 16, null)
                .blockingGet();
        end = System.currentTimeMillis();
        duration = end - start;
        System.out.println("Big blocks done in " + duration + "ms.");
        System.out.println("Status code: " + blockBlobCommitBlockListResponse.statusCode());

        start = System.currentTimeMillis();

        // Smaller blocks
        blockBlobCommitBlockListResponse =
            TransferManager.uploadFromNonReplayableFlowable(oneChunkFlowable, blobURL, SMALL_BLOCK_SIZE, 16, null)
                .blockingGet();
        end = System.currentTimeMillis();
        duration = end - start;
        System.out.println("Small blocks done in " + duration + "ms.");
        System.out.println("Status code: " + blockBlobCommitBlockListResponse.statusCode());

        start = System.currentTimeMillis();
        // Smaller blocks
        blockBlobCommitBlockListResponse =
            TransferManager.uploadFromNonReplayableFlowable(oneChunkFlowable, blobURL, REALLY_SMALL_BLOCK_SIZE, 16, null)
                .blockingGet();
        end = System.currentTimeMillis();
        duration = end - start;
        System.out.println("Really small blocks done in " + duration + "ms.");
        System.out.println("Status code: " + blockBlobCommitBlockListResponse.statusCode());
    }
}

