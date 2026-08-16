package com.jdragon.aggregation.datasource.file.s3.minio;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AliyunOssHelperTest {

    @Test
    public void listPageSkipsMarkerOnlyPageAndReturnsVisibleChild() throws Exception {
        OSS client = mock(OSS.class);
        ObjectListing markerPage = listing(List.of(summary("folder/", 0L)), true, "folder/");
        ObjectListing childPage = listing(List.of(summary("folder/item.txt", 10L)), false, null);
        when(client.listObjects(any(ListObjectsRequest.class))).thenReturn(markerPage, childPage);

        AliyunOssHelper helper = helper(client);
        TransferFilePage page = helper.listPage("/folder", null, 1);

        assertEquals(1, page.entries().size());
        assertEquals("item.txt", page.entries().get(0).name());
        assertFalse(page.entries().get(0).directory());
        assertFalse(page.truncated());
        assertNull(page.nextCursor());
        verify(client, times(2)).listObjects(any(ListObjectsRequest.class));
    }

    @Test
    public void listPageKeepsMarkerOnlyDirectoryEmptyWhenListingIsExhausted() throws Exception {
        OSS client = mock(OSS.class);
        ObjectListing markerPage = listing(List.of(summary("folder/", 0L)), false, null);
        when(client.listObjects(any(ListObjectsRequest.class))).thenReturn(markerPage);

        TransferFilePage page = helper(client).listPage("/folder", null, 1);

        assertEquals(0, page.entries().size());
        assertFalse(page.truncated());
        assertNull(page.nextCursor());
    }

    private AliyunOssHelper helper(OSS client) throws Exception {
        AliyunOssHelper helper = new AliyunOssHelper();
        setField(helper, "ossClient", client);
        setField(helper, "bucketName", "bucket");
        setField(helper, "rootPrefix", "");
        return helper;
    }

    private ObjectListing listing(List<OSSObjectSummary> summaries,
                                  boolean truncated,
                                  String nextMarker) {
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getCommonPrefixes()).thenReturn(List.of());
        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(listing.isTruncated()).thenReturn(truncated);
        when(listing.getNextMarker()).thenReturn(nextMarker);
        return listing;
    }

    private OSSObjectSummary summary(String key, long size) {
        OSSObjectSummary summary = mock(OSSObjectSummary.class);
        when(summary.getKey()).thenReturn(key);
        when(summary.getSize()).thenReturn(size);
        return summary;
    }

    private void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
