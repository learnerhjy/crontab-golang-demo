package common

import "errors"

var(
	LOCK_ALREADY_OCCUPIED_ERROR = errors.New("锁已经被占用")
	NO_LOCAL_IP_FOUND_ERROR = errors.New("无本地网卡地址")
)
