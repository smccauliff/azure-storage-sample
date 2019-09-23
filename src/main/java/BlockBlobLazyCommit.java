import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.BlockBlobCommitBlockListResponse;
import com.microsoft.azure.storage.blob.models.BlockBlobStageBlockResponse;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

public class BlockBlobLazyCommit {

  public static void main(String[] args) throws Exception {
    String accountName = args[0];
    String accountKey = args[1];

    byte[] backingByteBuffer = new byte[16];
    ByteBuffer bbuf = ByteBuffer.wrap(backingByteBuffer);
    Flowable<ByteBuffer> oneChunkFlowable =  Flowable.just(bbuf);

    // Create a BlockBlobURL object that wraps a blob's URL and a default pipeline.
    URL u = new URL(String.format(Locale.ROOT, "https://%s.blob.core.windows.net/", accountName));
    ServiceURL s = new ServiceURL(u,
            StorageURL.createPipeline(new SharedKeyCredentials(accountName, accountKey), new PipelineOptions()));
    ContainerURL containerURL = s.createContainerURL("newcontainer");
    String filename = "lazy-block-commit-blob";
    BlockBlobURL blobURL = containerURL.createBlockBlobURL(filename);

    try {
      List<String> blockIds = new ArrayList<>();
      writeBlocks(backingByteBuffer, oneChunkFlowable, blobURL, blockIds, 0, 2, 'a');

      // Commit blocks will fail if we attempt to commit less than all of the blocks.
      commitBlocks(blobURL, blockIds);

      //Read blob will fail if there are no committed blocks.
      readBlob(blobURL);

      writeBlocks(backingByteBuffer, oneChunkFlowable, blobURL, blockIds, 2, 2, 'a');
      // Read without commit will only read the committed blocks.
      readBlob(blobURL);

      commitBlocks(blobURL, blockIds);
      readBlob(blobURL);

      //Rewrite the second and third blocks
      System.out.println("Rewrite blocks.");
      //Don't want to accumulate more block ids because we can commit the same block multiple times.
      writeBlocks(backingByteBuffer, oneChunkFlowable, blobURL, new ArrayList<>(), 1, 2, 'f');

      commitBlocks(blobURL, blockIds);
      readBlob(blobURL);

      // Randomly reorder the blocks.
      System.out.println("Shuffle blocks.");
      Collections.shuffle(blockIds);
      commitBlocks(blobURL, blockIds);

      readBlob(blobURL);

    } finally {
      blobURL.delete();
    }

  }

  private static void writeBlocks(byte[] backingByteBuffer, Flowable<ByteBuffer> oneChunkFlowable, BlockBlobURL blobURL, List<String> blockIds, int start, int count, char fillStart) throws IOException {
    for (int blockId = start; blockId < (start + count); blockId++) {
      String paddedBlockId = String.format("%05d", blockId);
      Arrays.fill(backingByteBuffer, (byte) (blockId + fillStart));
      String b64BlockId = Base64.getEncoder().encodeToString(paddedBlockId.getBytes());
      BlockBlobStageBlockResponse blockBlobStageBlockResponse =
              blobURL.stageBlock(b64BlockId, oneChunkFlowable, backingByteBuffer.length).blockingGet();
      if (blockBlobStageBlockResponse.statusCode() < 200 || blockBlobStageBlockResponse.statusCode() >299) {
        throw new IOException("Bad status code " + blockBlobStageBlockResponse.statusCode());
      }
      blockIds.add(b64BlockId);
    }
  }

  private static void commitBlocks(BlockBlobURL blobURL, List<String> blockIds) throws IOException {
    BlockBlobCommitBlockListResponse commitResponse = blobURL.commitBlockList(blockIds).blockingGet();
    if (commitResponse.statusCode() < 200 || commitResponse.statusCode() > 299) {
      throw new IOException("Bad commit status code." + commitResponse);
    }
  }

  private static void readBlob(BlockBlobURL blobURL) {
    Single<DownloadResponse> download = blobURL.download();
    byte[] bytesRead = FlowableUtil.collectBytesInArray(download.blockingGet().body(null)).blockingGet();
    String asString = new String(bytesRead);
    System.out.println(asString);
  }
}
