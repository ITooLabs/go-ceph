package rados

// #cgo LDFLAGS: -lrados
// #include <errno.h>
// #include <stdlib.h>
// #include <rados/librados.h>
import "C"

import "unsafe"

const (
    LIBRADOS_CMPXATTR_OP_EQ  = 1
    LIBRADOS_CMPXATTR_OP_NE  = 2
    LIBRADOS_CMPXATTR_OP_GT  = 3
    LIBRADOS_CMPXATTR_OP_GTE = 4
    LIBRADOS_CMPXATTR_OP_LT  = 5
    LIBRADOS_CMPXATTR_OP_LTE = 6

    LIBRADOS_CREATE_EXCLUSIVE = 1
    LIBRADOS_CREATE_IDEMPOTENT = 0
)

func (ioctx *IOContext) ReadTaggedFull(oid string, tagName string, data []byte, offset int) (n int, tag []byte, err error) {
    if len(data) == 0 {
        return 0, nil, nil
    }

    c_oid := C.CString(oid)
    c_tagName := C.CString(tagName)
    defer C.free(unsafe.Pointer(c_oid))
    defer C.free(unsafe.Pointer(c_tagName))

    var size C.size_t
    var rval_read C.int
    var rval_attr C.int
    var it C.rados_xattrs_iter_t

    op := C.rados_create_read_op()
    defer func() { C.rados_release_read_op(op) }()

    C.rados_read_op_read(
        op,
        (C.uint64_t)(offset),
        (C.size_t)(len(data)),
        (*C.char)(unsafe.Pointer(&data[0])),
        &size,
        &rval_read)

    C.rados_read_op_getxattrs(
        op,
        &it,
        &rval_attr)
    
    ret := C.rados_read_op_operate(op, ioctx.ioctx, c_oid, 0)

    if ret < 0 {
        return 0, nil, GetRadosError(ret)
    }
    if rval_read < 0 {
        return 0, nil, GetRadosError(rval_read);
    }
    if rval_attr < 0 {
        return 0, nil, GetRadosError(ret)
    }

    defer func() { C.rados_getxattrs_end(it) }()

    for {
        var c_name, c_val *C.char
        var c_len C.size_t
        defer C.free(unsafe.Pointer(c_name))
        defer C.free(unsafe.Pointer(c_val))

        ret := C.rados_getxattrs_next(it, &c_name, &c_val, &c_len)
        if ret < 0 {
            return int(size), nil, GetRadosError(ret)
        }
        // rados api returns a null name,val & 0-length upon
        // end of iteration
        if c_name == nil {
            return int(size), nil, GetRadosError(ret)
        }
        if  tagName == C.GoString(c_name) {
            tag = C.GoBytes(unsafe.Pointer(c_val), (C.int)(c_len))
            break
        }
    }

    return int(size), tag, nil

}

func (ioctx *IOContext) WriteTaggedFull(oid string, tagName string, tag string, newTag string, data []byte) error {
    c_oid := C.CString(oid)
    c_tagName := C.CString(tagName)
    defer C.free(unsafe.Pointer(c_oid))
    defer C.free(unsafe.Pointer(c_tagName))

    b_tag := []byte(tag)
    b_newTag := []byte(newTag)

    op := C.rados_create_write_op()

    if tag == "" || tag == "0" {
        C.rados_write_op_create(
            op,
            LIBRADOS_CREATE_EXCLUSIVE,
            nil)
    } else {
        C.rados_write_op_cmpxattr(
            op,
            c_tagName,
            LIBRADOS_CMPXATTR_OP_EQ,
            (*C.char)(unsafe.Pointer(&b_tag[0])),
            (C.size_t)(len(b_tag)))
    }
    
    C.rados_write_op_write_full(
        op,
        (*C.char)(unsafe.Pointer(&data[0])),
        (C.size_t)(len(data)))

    C.rados_write_op_setxattr(
        op,
        c_tagName,
        (*C.char)(unsafe.Pointer(&b_newTag[0])),
        (C.size_t)(len(b_newTag)))
    
    ret := C.rados_write_op_operate(op, ioctx.ioctx, c_oid, nil, 0)
    
    C.rados_release_write_op(op)

    return GetRadosError(ret)
}