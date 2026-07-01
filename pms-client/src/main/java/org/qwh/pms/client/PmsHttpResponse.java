package org.qwh.pms.client;

record PmsHttpResponse(int statusCode, byte[] body) {}
