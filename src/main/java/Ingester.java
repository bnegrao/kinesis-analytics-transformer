import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisAnalyticsInputPreprocessingResponse;
import com.amazonaws.services.lambda.runtime.events.KinesisAnalyticsStreamsInputPreprocessingEvent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class Ingester implements
        RequestHandler<KinesisAnalyticsStreamsInputPreprocessingEvent, KinesisAnalyticsInputPreprocessingResponse> {
    @Override
    public KinesisAnalyticsInputPreprocessingResponse handleRequest(
            KinesisAnalyticsStreamsInputPreprocessingEvent event, Context context) {
        LambdaLogger logger = context.getLogger();

        logger.log("InvocatonId is : " + event.invocationId);
        logger.log("StreamArn is : " + event.streamArn);
        logger.log("ApplicationArn is : " + event.applicationArn);

        List<KinesisAnalyticsInputPreprocessingResponse.Record> records = new ArrayList<KinesisAnalyticsInputPreprocessingResponse.Record>();
        KinesisAnalyticsInputPreprocessingResponse response = new KinesisAnalyticsInputPreprocessingResponse(records);

        event.records.stream().forEach(record -> {
            logger.log("recordId is : " + record.recordId);

            KinesisAnalyticsInputPreprocessingResponse.Record responseRecord = new KinesisAnalyticsInputPreprocessingResponse.Record();
            responseRecord.recordId = record.recordId;

            try {
                ByteBuffer decompressedData = decompress(record.getData());
                logger.log(StandardCharsets.UTF_8.decode(decompressedData).toString());
                responseRecord.data = decompressedData;

                responseRecord.result = KinesisAnalyticsInputPreprocessingResponse.Result.Ok;
            }catch (IOException ex) {
                logger.log("ERROR: " + ex.getMessage());
                responseRecord.result = KinesisAnalyticsInputPreprocessingResponse.Result.ProcessingFailed;
                responseRecord.data = record.data;
            }

            response.records.add(responseRecord);
        });
        return response;
    }

    private ByteBuffer decompress (ByteBuffer compressed) throws IOException {
        ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(compressed);
        GZIPInputStream gzis = new GZIPInputStream(byteBufferInputStream);
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int len;
        while ((len = gzis.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
        return ByteBuffer.wrap(out.toByteArray());
    }
}
