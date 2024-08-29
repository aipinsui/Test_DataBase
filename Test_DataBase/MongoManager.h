#pragma once

#include <QObject>
#include <QThread>

#include "mongoc/mongoc.h"

class MongoManager : public QObject
{
	Q_OBJECT

public:
	MongoManager(QString qstrURI, QString qstrDatabaseName, int nPlaneID, QObject* parent);
	MongoManager(QString qstrURI, QString qstrDatabaseName, QObject* parent);
	~MongoManager();

public:
	void SetImportType(int nImportType);

	void Start();

	void Query();

private:
	bool CreateConn();

	void CleanConn();

	void InsertData(QString qstrInfo);

	void BulkOperation(QString qstrInfo);

signals:
	void sig_startThread();

	void sig_finishTask(bool bSuccess, int nPlaneID, QString qstrInfo);

	void sig_finishTask(bool bSuccess, QString qstrInfo);

public slots:
	void slot_startTask();

	void slot_startQuery();

private:
	QString m_qstrURI;

	QString m_qstrDatabaseName;

	int m_nPlaneID;

	int m_nImportType;

	QString m_qstrInfo;

	QThread* m_pThread;//数据入库线程

	mongoc_client_t* m_pMongoClient;//数据库客户端句柄

	mongoc_database_t* m_pMongoDB;//数据库句柄

	mongoc_collection_t* m_pMongoCollection;//数据库集合句柄

	mongoc_gridfs_bucket_t* m_pGridfsBucket;//数据库存储桶句柄
};
