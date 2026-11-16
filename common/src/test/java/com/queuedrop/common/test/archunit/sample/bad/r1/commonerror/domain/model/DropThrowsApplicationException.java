package com.queuedrop.common.test.archunit.sample.bad.r1.commonerror.domain.model;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;

public class DropThrowsApplicationException {
    public void validate() { throw new ApplicationException(CommonErrorCode.INVALID_INPUT); }
}
