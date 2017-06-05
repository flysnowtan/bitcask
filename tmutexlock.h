#pragma once

#include <pthread.h>

class TMutexLock
{
public:
	TMutexLock()
	{
		pthread_mutex_init(&m_Lock, NULL);
	}

	~TMutexLock()
	{
		pthread_mutex_destroy(&m_Lock);
	}

	pthread_mutex_t * GetLock()
	{
		return &m_Lock;
	}

private:
	pthread_mutex_t m_Lock;
};

class TMutexLockHelper
{
public:
	TMutexLockHelper(TMutexLock &lock)
		:m_pLock(&lock)
	{
		pthread_mutex_lock(m_pLock->GetLock());
	}

	~TMutexLockHelper()
	{
		pthread_mutex_unlock(m_pLock->GetLock());
	}

private:
	TMutexLock * m_pLock;
};

