/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.provider.amqp.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import org.apache.qpid.jms.exceptions.IdConversionException;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.junit.Before;
import org.junit.Test;

public class AmqpMessageIdHelperTest extends QpidJmsTestCase {
    private AmqpMessageIdHelper _messageIdHelper;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        _messageIdHelper = new AmqpMessageIdHelper();
    }

    /**
     * Test that {@link AmqpMessageIdHelper#hasMessageIdPrefix(String)} returns true for strings that begin "ID:"
     */
    @Test
    public void testHasIdPrefixWithPrefix() {
        String myId = "ID:something";
        assertTrue("'ID:' prefix should have been identified", _messageIdHelper.hasMessageIdPrefix(myId));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#hasMessageIdPrefix(String)} returns false for string beings "ID" without colon.
     */
    @Test
    public void testHasIdPrefixWithIDButNoColonPrefix() {
        String myIdNoColon = "IDsomething";
        assertFalse("'ID' prefix should not have been identified without trailing colon", _messageIdHelper.hasMessageIdPrefix(myIdNoColon));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#hasMessageIdPrefix(String)} returns false for null
     */
    @Test
    public void testHasIdPrefixWithNull() {
        String nullString = null;
        assertFalse("null string should not result in identification as having the prefix", _messageIdHelper.hasMessageIdPrefix(nullString));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#hasMessageIdPrefix(String)} returns false for strings that doesnt have "ID:" anywhere
     */
    @Test
    public void testHasIdPrefixWithoutPrefix() {
        String myNonId = "something";
        assertFalse("string without 'ID:' anywhere should not have been identified as having the prefix", _messageIdHelper.hasMessageIdPrefix(myNonId));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#hasMessageIdPrefix(String)} returns false for strings has lowercase "id:" prefix
     */
    @Test
    public void testHasIdPrefixWithLowercaseID() {
        String myLowerCaseNonId = "id:something";
        assertFalse("lowercase 'id:' prefix should not result in identification as having 'ID:' prefix", _messageIdHelper.hasMessageIdPrefix(myLowerCaseNonId));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#stripMessageIdPrefix(String)} strips "ID:" from strings that do begin "ID:"
     */
    @Test
    public void testStripMessageIdPrefixWithPrefix() {
        String myIdWithoutPrefix = "something";
        String myId = "ID:" + myIdWithoutPrefix;
        assertEquals("'ID:' prefix should have been stripped", myIdWithoutPrefix, _messageIdHelper.stripMessageIdPrefix(myId));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#stripMessageIdPrefix(String)} only strips one "ID:" from strings that
     * begin "ID:ID:...."
     */
    @Test
    public void testStripMessageIdPrefixWithDoublePrefix() {
        String myIdWithSinglePrefix = "ID:something";
        String myIdWithDoublePrefix = "ID:" + myIdWithSinglePrefix;
        assertEquals("'ID:' prefix should only have been stripped once", myIdWithSinglePrefix, _messageIdHelper.stripMessageIdPrefix(myIdWithDoublePrefix));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#stripMessageIdPrefix(String)} does not alter strings that begins "ID" without a colon.
     */
    @Test
    public void testStripMessageIdPrefixWithIDButNoColonPrefix() {
        String myIdNoColon = "IDsomething";
        assertEquals("string without 'ID:' prefix should have been returned unchanged", myIdNoColon, _messageIdHelper.stripMessageIdPrefix(myIdNoColon));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#stripMessageIdPrefix(String)} returns null if given null;
     */
    @Test
    public void testStripMessageIdPrefixWithNull() {
        String nullString = null;
        assertNull("null string should have been returned", _messageIdHelper.stripMessageIdPrefix(nullString));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#stripMessageIdPrefix(String)} does not alter string that doesn't begin "ID:"
     */
    @Test
    public void testStripMessageIdPrefixWithoutIDAnywhere() {
        String myNonId = "something";
        assertEquals("string without 'ID:' anywhere should have been returned unchanged", myNonId, _messageIdHelper.stripMessageIdPrefix(myNonId));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#stripMessageIdPrefix(String)} does not alter string with lowercase "id:"
     */
    @Test
    public void testStripMessageIdPrefixWithLowercaseID() {
        String myLowerCaseNonId = "id:something";
        assertEquals("string with lowercase 'id:' prefix should have been returned unchanged", myLowerCaseNonId, _messageIdHelper.stripMessageIdPrefix(myLowerCaseNonId));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns null if given null
     */
    @Test
    public void testToMessageIdStringWithNull() {
        assertNull("null string should have been returned", _messageIdHelper.toMessageIdString(null));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} throws an IAE if given an unexpected object type.
     */
    @Test
    public void testToMessageIdStringThrowsIAEWithUnexpectedType() {
        try {
            _messageIdHelper.toMessageIdString(new Object());
            fail("expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns the given
     * basic string unchanged
     */
    @Test
    public void testToMessageIdStringWithString() {
        String stringId = "ID:myIdString";

        String idString = _messageIdHelper.toMessageIdString(stringId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", stringId, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns the given
     * basic string unchanged
     */
    @Test
    public void testToMessageIdStringWithStringNoPrefix() {
        String stringId = "myIdStringNoPrefix";
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_NO_PREFIX + stringId;

        String idString = _messageIdHelper.toMessageIdString(stringId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns a string
     * indicating an AMQP encoded string, when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_UUID_PREFIX}.
     */
    @Test
    public void testToMessageIdStringWithStringBeginningWithEncodingPrefixForUUID() {
        String uuidStringMessageId =  AmqpMessageIdHelper.AMQP_UUID_PREFIX + UUID.randomUUID();
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + uuidStringMessageId;

        String idString = _messageIdHelper.toMessageIdString(uuidStringMessageId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns a string
     * indicating an AMQP encoded string, when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_ULONG_PREFIX}.
     */
    @Test
    public void testToBaseMessageIdStringWithStringBeginningWithEncodingPrefixForLong() {
        String longStringMessageId = AmqpMessageIdHelper.AMQP_ULONG_PREFIX + Long.valueOf(123456789L);
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + longStringMessageId;

        String baseMessageIdString = _messageIdHelper.toMessageIdString(longStringMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns a string
     * indicating an AMQP encoded string, when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_BINARY_PREFIX}.
     */
    @Test
    public void testToMessageIdStringWithStringBeginningWithEncodingPrefixForBinary() {
        String binaryStringMessageId = AmqpMessageIdHelper.AMQP_BINARY_PREFIX + "0123456789ABCDEF";
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + binaryStringMessageId;

        String baseMessageIdString = _messageIdHelper.toMessageIdString(binaryStringMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns a string
     * indicating an AMQP encoded string (effectively twice), when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_STRING_PREFIX}.
     */
    @Test
    public void testToMessageIdStringWithStringBeginningWithEncodingPrefixForString() {
        String stringMessageId = AmqpMessageIdHelper.AMQP_STRING_PREFIX + "myStringId";
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + stringMessageId;

        String baseMessageIdString = _messageIdHelper.toMessageIdString(stringMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns a string
     * indicating an AMQP encoded string (effectively twice), when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_NO_PREFIX}.
     */
    @Test
    public void testToMessageIdStringWithStringBeginningWithEncodingPrefixForNoIdPrefix() {
        String stringMessageId = AmqpMessageIdHelper.AMQP_NO_PREFIX + "myStringId";
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + stringMessageId;

        String baseMessageIdString = _messageIdHelper.toMessageIdString(stringMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns a string
     * indicating an AMQP encoded UUID when given a UUID object.
     */
    @Test
    public void testToMessageIdStringWithUUID() {
        UUID uuidMessageId = UUID.randomUUID();
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX + uuidMessageId.toString();

        String idString = _messageIdHelper.toMessageIdString(uuidMessageId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns a string
     * indicating an AMQP encoded ulong when given a UnsignedLong object.
     */
    @Test
    public void testToMessageIdStringWithUnsignedLong() {
        UnsignedLong uLongMessageId = UnsignedLong.valueOf(123456789L);
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX + uLongMessageId.toString();

        String idString = _messageIdHelper.toMessageIdString(uLongMessageId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toMessageIdString(Object)} returns a string
     * indicating an AMQP encoded binary when given a Binary object.
     */
    @Test
    public void testToMessageIdStringWithBinary() {
        byte[] bytes = new byte[] { (byte) 0x00, (byte) 0xAB, (byte) 0x09, (byte) 0xFF };
        Binary binary = new Binary(bytes);

        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + "00AB09FF";

        String idString = _messageIdHelper.toMessageIdString(binary);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected base id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns null if given null
     */
    @Test
    public void testToCorrelationIdStringWithNull() {
        assertNull("null string should have been returned", _messageIdHelper.toCorrelationIdString(null));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} throws an IAE if given an unexpected object type.
     */
    @Test
    public void testToCorrelationIdStringThrowsIAEWithUnexpectedType() {
        try {
            _messageIdHelper.toCorrelationIdString(new Object());
            fail("expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns the given
     * basic string unchanged
     */
    @Test
    public void testToCorrelationIdStringWithString() {
        String stringId = "ID:myIdString";

        String idString = _messageIdHelper.toCorrelationIdString(stringId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", stringId, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns the given
     * basic string unchanged
     */
    @Test
    public void testToCorrelationIdStringWithStringNoPrefix() {
        String stringId = "myIdString";

        String idString = _messageIdHelper.toCorrelationIdString(stringId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", stringId, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns a string
     * indicating an AMQP encoded string, when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_UUID_PREFIX}.
     */
    @Test
    public void testToCorrelationIdStringWithStringBeginningWithEncodingPrefixForUUID() {
        String uuidStringMessageId =  AmqpMessageIdHelper.AMQP_UUID_PREFIX + UUID.randomUUID();
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + uuidStringMessageId;

        String idString = _messageIdHelper.toCorrelationIdString(uuidStringMessageId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns a string
     * indicating an AMQP encoded string, when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_ULONG_PREFIX}.
     */
    @Test
    public void testToCorrelationIdStringWithStringBeginningWithEncodingPrefixForLong() {
        String longStringCorrelationId = AmqpMessageIdHelper.AMQP_ULONG_PREFIX + Long.valueOf(123456789L);
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + longStringCorrelationId;

        String idString = _messageIdHelper.toCorrelationIdString(longStringCorrelationId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns a string
     * indicating an AMQP encoded string, when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_BINARY_PREFIX}.
     */
    @Test
    public void testToCorrelationIdStringWithStringBeginningWithEncodingPrefixForBinary() {
        String binaryStringCorrelationId = AmqpMessageIdHelper.AMQP_BINARY_PREFIX + "0123456789ABCDEF";
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + binaryStringCorrelationId;

        String idString = _messageIdHelper.toCorrelationIdString(binaryStringCorrelationId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns a string
     * indicating an AMQP encoded string (effectively twice), when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_STRING_PREFIX}.
     */
    @Test
    public void testToCorrelationIdStringWithStringBeginningWithEncodingPrefixForString() {
        String stringCorrelationId = AmqpMessageIdHelper.AMQP_STRING_PREFIX + "myStringId";
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + stringCorrelationId;

        String idString = _messageIdHelper.toCorrelationIdString(stringCorrelationId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns a string
     * indicating an AMQP encoded string (effectively twice), when the given string happens to already begin with
     * the {@link AmqpMessageIdHelper#AMQP_NO_PREFIX}.
     */
    @Test
    public void testToCorrelationIdStringWithStringBeginningWithEncodingPrefixForNoIdPrefix() {
        String stringCorrelationId = AmqpMessageIdHelper.AMQP_NO_PREFIX + "myStringId";
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + stringCorrelationId;

        String idString = _messageIdHelper.toCorrelationIdString(stringCorrelationId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns a string
     * indicating an AMQP encoded UUID when given a UUID object.
     */
    @Test
    public void testToCorrelationIdStringWIdStringWithUUID() {
        UUID uuidCorrelationId = UUID.randomUUID();
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX + uuidCorrelationId.toString();

        String idString = _messageIdHelper.toCorrelationIdString(uuidCorrelationId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns a string
     * indicating an AMQP encoded ulong when given a UnsignedLong object.
     */
    @Test
    public void testToCorrelationIdStringWithUnsignedLong() {
        UnsignedLong uLongCorrelationId = UnsignedLong.valueOf(123456789L);
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX + uLongCorrelationId.toString();

        String idString = _messageIdHelper.toCorrelationIdString(uLongCorrelationId);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toCorrelationIdString(Object)} returns a string
     * indicating an AMQP encoded binary when given a Binary object.
     */
    @Test
    public void testToCorrelationIdStringWithBinary() {
        byte[] bytes = new byte[] { (byte) 0x00, (byte) 0xAB, (byte) 0x09, (byte) 0xFF };
        Binary binary = new Binary(bytes);

        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + "00AB09FF";

        String idString = _messageIdHelper.toCorrelationIdString(binary);
        assertNotNull("null string should not have been returned", idString);
        assertEquals("expected base id string was not returned", expected, idString);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} returns an
     * UnsignedLong when given a string indicating an encoded AMQP ulong id.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToIdObjectWithEncodedUlong() throws Exception {
        UnsignedLong longId = UnsignedLong.valueOf(123456789L);
        String provided = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX + "123456789";

        Object idObject = _messageIdHelper.toIdObject(provided);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", longId, idObject);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} returns a Binary
     * when given a string indicating an encoded AMQP binary id, using upper case hex characters
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToIdObjectWithEncodedBinaryUppercaseHexString() throws Exception {
        byte[] bytes = new byte[] { (byte) 0x00, (byte) 0xAB, (byte) 0x09, (byte) 0xFF };
        Binary binaryId = new Binary(bytes);

        String provided = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + "00AB09FF";

        Object idObject = _messageIdHelper.toIdObject(provided);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", binaryId, idObject);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} returns null
     * when given null.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToIdObjectWithNull() throws Exception {
        assertNull("null object should have been returned", _messageIdHelper.toIdObject(null));
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} returns a Binary
     * when given a string indicating an encoded AMQP binary id, using lower case hex characters.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToIdObjectWithEncodedBinaryLowercaseHexString() throws Exception {
        byte[] bytes = new byte[] { (byte) 0x00, (byte) 0xAB, (byte) 0x09, (byte) 0xFF };
        Binary binaryId = new Binary(bytes);

        String provided = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + "00ab09ff";

        Object idObject = _messageIdHelper.toIdObject(provided);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", binaryId, idObject);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} returns a UUID
     * when given a string indicating an encoded AMQP uuid id.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToIdObjectWithEncodedUuid() throws Exception {
        UUID uuid = UUID.randomUUID();
        String provided = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX + uuid.toString();

        Object idObject = _messageIdHelper.toIdObject(provided);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", uuid, idObject);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} returns a string
     * when given a string without any type encoding prefix.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToIdObjectWithStringContainingNoEncodingPrefix() throws Exception {
        String stringId = "myStringId";

        Object idObject = _messageIdHelper.toIdObject(stringId);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", stringId, idObject);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} returns the remainder of the
     * provided string after removing the {@link AmqpMessageIdHelper#AMQP_STRING_PREFIX} prefix.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToIdObjectWithStringContainingStringEncodingPrefix() throws Exception {
        String suffix = "myStringSuffix";
        String stringId = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + suffix;

        Object idObject = _messageIdHelper.toIdObject(stringId);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", suffix, idObject);
    }

    /**
     * Test that when given a string with with the {@link AmqpMessageIdHelper#AMQP_STRING_PREFIX} prefix
     * and then additionally the {@link AmqpMessageIdHelper#AMQP_UUID_PREFIX}, the
     * {@link AmqpMessageIdHelper#toIdObject(String)} method returns the remainder of the provided string
     *  after removing the {@link AmqpMessageIdHelper#AMQP_STRING_PREFIX} prefix.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToIdObjectWithStringContainingStringEncodingPrefixAndThenUuidPrefix() throws Exception {
        String encodedUuidString = AmqpMessageIdHelper.AMQP_UUID_PREFIX + UUID.randomUUID().toString();
        String stringId = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_STRING_PREFIX + encodedUuidString;

        Object idObject = _messageIdHelper.toIdObject(stringId);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", encodedUuidString, idObject);
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} throws an
     * {@link IdConversionException} when presented with an encoded binary hex string
     * of uneven length (after the prefix) that thus can't be converted due to each
     * byte using 2 characters
     */
    @Test
    public void testToIdObjectWithStringContainingBinaryHexThrowsICEWithUnevenLengthString() {
        String unevenHead = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + "123";

        try {
            _messageIdHelper.toIdObject(unevenHead);
            fail("expected exception was not thrown");
        } catch (IdConversionException iae) {
            // expected
            String msg = iae.getCause().getMessage();
            assertTrue("Message was not as expected: " + msg, msg.contains("even length"));
        }
    }

    /**
     * Test that {@link AmqpMessageIdHelper#toIdObject(String)} throws an
     * {@link IdConversionException} when presented with an encoded binary hex
     * string (after the prefix) that contains characters other than 0-9
     * and A-F and a-f, and thus can't be converted
     */
    @Test
    public void testToIdObjectWithStringContainingBinaryHexThrowsICEWithNonHexCharacters() {

        // char before '0'
        char nonHexChar = '/';
        String nonHexString = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + nonHexChar + nonHexChar;

        try {
            _messageIdHelper.toIdObject(nonHexString);
            fail("expected exception was not thrown");
        } catch (IdConversionException ice) {
            // expected
            String msg = ice.getCause().getMessage();
            assertTrue("Message was not as expected: " + msg, msg.contains("non-hex"));
        }

        // char after '9', before 'A'
        nonHexChar = ':';
        nonHexString = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + nonHexChar + nonHexChar;

        try {
            _messageIdHelper.toIdObject(nonHexString);
            fail("expected exception was not thrown");
        } catch (IdConversionException ice) {
            // expected
            String msg = ice.getCause().getMessage();
            assertTrue("Message was not as expected: " + msg, msg.contains("non-hex"));
        }

        // char after 'F', before 'a'
        nonHexChar = 'G';
        nonHexString = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + nonHexChar + nonHexChar;

        try {
            _messageIdHelper.toIdObject(nonHexString);
            fail("expected exception was not thrown");
        } catch (IdConversionException ice) {
            // expected
            String msg = ice.getCause().getMessage();
            assertTrue("Message was not as expected: " + msg, msg.contains("non-hex"));
        }

        // char after 'f'
        nonHexChar = 'g';
        nonHexString = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX + nonHexChar + nonHexChar;

        try {
            _messageIdHelper.toIdObject(nonHexString);
            fail("expected exception was not thrown");
        } catch (IdConversionException ice) {
            // expected
            String msg = ice.getCause().getMessage();
            assertTrue("Message was not as expected: " + msg, msg.contains("non-hex"));
        }
    }
}
