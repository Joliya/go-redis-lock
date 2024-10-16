// Copyright 2022 <mzh.scnu@qq.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tools

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
)

const (
	// defaultExp  默认的锁超时时间
	defaultExp = 10 * time.Second

	// sleepDur 默认的自旋锁休眠时间
	sleepDur = 10 * time.Millisecond
)

// RedisLock 结构体定义
type RedisLock struct {
	Client     RedisClient
	Key        string // 需要锁定的资源
	uuid       string // 锁的拥有者UUID
	cancelFunc context.CancelFunc
}

// NewRedisLock 创建一个新的Redis分布式锁
func NewRedisLock(client RedisClient, key string) (*RedisLock, error) {
	id, err := uuid.NewV4() // 生成一个新的UUID
	if err != nil {
		return nil, err
	}
	return &RedisLock{
		Client: client,
		Key:    key,
		uuid:   id.String(),
	}, nil
}

// TryLock 尝试加锁，如果加锁成功返回true，否则返回false
func (rl *RedisLock) TryLock(ctx context.Context) (bool, error) {
	succ, err := rl.Client.SetNX(ctx, rl.Key, rl.uuid, defaultExp).Result() // 尝试设置NX键值对
	if err != nil || !succ {
		return false, err
	}
	c, cancel := context.WithCancel(ctx) // 创建一个可取消的上下文
	rl.cancelFunc = cancel
	rl.refresh(c) // 刷新锁的过期时间
	return succ, nil
}

// SpinLock 循环`retryTimes`次调用TryLock
func (rl *RedisLock) SpinLock(ctx context.Context, retryTimes int) (bool, error) {
	for i := 0; i < retryTimes; i++ {
		resp, err := rl.TryLock(ctx) // 尝试加锁
		if err != nil {
			return false, err
		}
		if resp {
			return resp, nil
		}
		time.Sleep(sleepDur) // 休眠一段时间后重试
	}
	return false, nil
}

// Unlock 尝试解锁，如果解锁成功返回true，否则返回false
func (rl *RedisLock) Unlock(ctx context.Context) (bool, error) {
	resp, err := NewTools(rl.Client).Cad(ctx, rl.Key, rl.uuid) // 尝试删除键值对
	if err != nil {
		return false, err
	}

	if resp {
		rl.cancelFunc() // 取消刷新锁的上下文
	}
	return resp, nil
}

// refresh 刷新锁的过期时间
func (rl *RedisLock) refresh(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(defaultExp / 4) // 每隔一段时间刷新一次
		for {
			select {
			case <-ctx.Done(): // 上下文取消时退出
				return
			case <-ticker.C: // 定时刷新锁的过期时间
				rl.Client.Expire(ctx, rl.Key, defaultExp)
			}
		}
	}()
}
