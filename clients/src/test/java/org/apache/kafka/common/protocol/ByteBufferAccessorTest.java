/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.protocol;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteBufferAccessorTest {

    @Test
    public void testReadWrite() {
        byte[] data = Utils.utf8("foo");
        ByteBuffer zeroCopyBuffer = ByteBuffer.wrap(data);
        ByteBufferAccessor builder = new ByteBufferAccessor(ByteBuffer.allocate(8 + zeroCopyBuffer.remaining()));

        builder.writeInt(5);
        builder.writeByteBuffer(zeroCopyBuffer);
        builder.writeInt(15);

        builder.flip();

        assertEquals(5, builder.readInt());
        assertEquals("foo", TestUtils.getString(builder.readByteBuffer(data.length), data.length));
        assertEquals(15, builder.readInt());
    }

    @Test
    public void testCopyByteBuffer() {
        byte[] data = Utils.utf8("foo");
        ByteBuffer zeroCopyBuffer = ByteBuffer.wrap(data);
        ByteBufferAccessor builder = new ByteBufferAccessor(ByteBuffer.allocate(8 + zeroCopyBuffer.remaining()));

        builder.writeInt(5);
        builder.writeByteBuffer(zeroCopyBuffer);
        builder.writeInt(15);
        builder.flip();

        // Overwrite the original buffer in order to prove the data was copied
        byte[] overwrittenData = Utils.utf8("bar");
        assertEquals(data.length, overwrittenData.length);
        zeroCopyBuffer.rewind();
        zeroCopyBuffer.put(overwrittenData);
        zeroCopyBuffer.rewind();

        ByteBuffer buffer = builder.buffer();
        assertEquals(8 + data.length, buffer.remaining());
        assertEquals(5, buffer.getInt());

        assertEquals("foo", TestUtils.getString(buffer, data.length));
        assertEquals(15, buffer.getInt());
    }

    @Test
    public void testWriteByteBufferRespectsPosition() {
        byte[] data = Utils.utf8("yolo");
        assertEquals(4, data.length);

        ByteBuffer buffer = ByteBuffer.wrap(data);
        ByteBufferAccessor builder = new ByteBufferAccessor(ByteBuffer.allocate(buffer.remaining()));

        buffer.limit(2);
        builder.writeByteBuffer(buffer);
        assertEquals(0, buffer.position());

        buffer.position(2);
        buffer.limit(4);
        builder.writeByteBuffer(buffer);
        assertEquals(2, buffer.position());
        builder.flip();

        ByteBuffer readBuffer = builder.buffer();
        assertEquals("yolo", TestUtils.getString(readBuffer, 4));
    }

    @Test
    public void testCopyRecords() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        MemoryRecords records = TestUtils.createRecords(buffer, "foo");

        ByteBufferAccessor builder = new ByteBufferAccessor(ByteBuffer.allocate(records.sizeInBytes() + 8));
        builder.writeInt(5);
        builder.writeRecords(records);
        builder.writeInt(15);
        builder.flip();
        ByteBuffer readBuffer = builder.buffer();

        // Overwrite the original buffer in order to prove the data was copied
        buffer.rewind();
        MemoryRecords overwrittenRecords = TestUtils.createRecords(buffer, "bar");

        assertEquals(5, readBuffer.getInt());
        assertEquals(records, overwrittenRecords);
        assertEquals(TestUtils.createRecords(ByteBuffer.allocate(128), "foo"), TestUtils.getRecords(readBuffer, records.sizeInBytes()));

        assertEquals(15, readBuffer.getInt());
    }

}
