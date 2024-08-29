#pragma once

#include <QObject>
#include <QThread>

#include "jdbc/cppconn/driver.h"

class MySQLManager : public QObject
{
	Q_OBJECT

public:
	MySQLManager(sql::Driver* pDriver, QString qstrHostName, QString qstrUserName, QString qstrPassword, int nPlaneID, QObject *parent);
	~MySQLManager();

public:
	void Start();

private:
	bool createConn();

	void cleanConn();

signals:
	void sig_startThread();

	void sig_finishTask(bool bSuccess, int nPlaneID, QString qstrInfo);

public slots:
	void slot_startTask();

private:
	QString m_qstrHostName;

	QString m_qstrUserName;

	QString m_qstrPassword;

	int m_nPlaneID;

	QString m_qstrInfo;

	QThread* m_pThread;//数据入库线程

	sql::Driver* m_pDriver;

	sql::Connection* m_pConn;
};
