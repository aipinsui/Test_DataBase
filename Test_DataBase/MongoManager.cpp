#include "MongoManager.h"

#include <QApplication>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonArray>
#include <QDataStream>

MongoManager::MongoManager(QString qstrURI, QString qstrDatabaseName, int nPlaneID, QObject* parent)
	: QObject(parent)
{
	m_qstrURI = qstrURI;
	m_qstrDatabaseName = qstrDatabaseName;
	m_nPlaneID = nPlaneID;
	m_nImportType = 0;//0�����ĵ���1����ͼƬ
	m_pThread = NULL;
	m_pMongoClient = NULL;
	m_pMongoDB = NULL;
	m_pMongoCollection = NULL;
	m_pGridfsBucket = NULL;
}

MongoManager::MongoManager(QString qstrURI, QString qstrDatabaseName, QObject* parent)
	: QObject(parent)
{
	m_qstrURI = qstrURI;
	m_qstrDatabaseName = qstrDatabaseName;
	m_nImportType = 0;//0�����ĵ���1����ͼƬ
	m_pThread = NULL;
	m_pMongoClient = NULL;
	m_pMongoDB = NULL;
	m_pMongoCollection = NULL;
	m_pGridfsBucket = NULL;
}

MongoManager::~MongoManager()
{

}

void MongoManager::SetImportType(int nImportType)
{
	m_nImportType = nImportType;
}

void MongoManager::Start()
{
	//���������߳�
	m_pThread = new QThread();
	connect(this, SIGNAL(sig_startThread()), this, SLOT(slot_startTask()));
	this->moveToThread(m_pThread);
	m_pThread->start();
	emit sig_startThread();
}

void MongoManager::Query()
{
	//���������߳�
	m_pThread = new QThread();
	connect(this, SIGNAL(sig_startThread()), this, SLOT(slot_startQuery()));
	this->moveToThread(m_pThread);
	m_pThread->start();
	emit sig_startThread();
}

bool MongoManager::CreateConn()
{
	//����Mongo���ݿ�
	QByteArray byteURI = m_qstrURI.toUtf8();
	const char* pURI = byteURI.constData();
	m_pMongoClient = mongoc_client_new(pURI);
	if (m_pMongoClient == NULL)
	{
		return false;
	}
	//��ȡָ�����ݿ�
	QByteArray byteDatabaseName = m_qstrDatabaseName.toUtf8();
	const char* pDatabaseName = byteDatabaseName.constData();
	m_pMongoDB = mongoc_client_get_database(m_pMongoClient, pDatabaseName);
	if (m_pMongoDB == NULL)
	{
		return false;
	}
	bson_error_t curError;
	if (m_nImportType == 0)
	{
		//��ȡָ�����ݿ�ָ������
		QString qstrCollecttionName = QString::fromLocal8Bit("aircraft_info_") + QString::number(m_nPlaneID);
		QByteArray byteCollecttionName = qstrCollecttionName.toUtf8();
		const char* pCollectionName = byteCollecttionName.constData();
		m_pMongoCollection = mongoc_database_get_collection(m_pMongoDB, pCollectionName);
		if (m_pMongoCollection == NULL)
		{
			m_pMongoCollection = mongoc_database_create_collection(m_pMongoDB, pCollectionName, NULL, &curError);
			if (m_pMongoCollection == NULL)
			{
				return false;
			}
		}
	}
	else if (m_nImportType == 1)
	{
		//���÷ֿ�ѡ�����
		bson_t* optionBson = bson_new();
		QString qstrBucketKey = QString::fromLocal8Bit("bucketName");
		QByteArray byteBucketKey = qstrBucketKey.toUtf8();
		const char* pBucketKey = byteBucketKey.constData();
		QString qstrBucketValue = QString::fromLocal8Bit("�����ϴ�ͼƬ");
		QByteArray byteBucketValue = qstrBucketValue.toUtf8();
		const char* pBucketValue = byteBucketValue.constData();
		bson_append_utf8(optionBson, pBucketKey, (int)strlen(pBucketKey), pBucketValue, (int)strlen(pBucketValue));//���ô洢Ͱ����
		QString qstrChunkKey = QString::fromLocal8Bit("chunkSizeBytes");
		QByteArray byteChunkKey = qstrChunkKey.toUtf8();
		const char* pChunkKey = byteChunkKey.constData();
		int32_t nChunkSize = 100 * 1024 * 1024;//100MB
		bson_append_int32(optionBson, pChunkKey, (int)strlen(pChunkKey), nChunkSize);//����ÿ����ռ�ֽ���
		mongoc_write_concern_t* pWriteConcern = mongoc_write_concern_new();
		mongoc_write_concern_set_w(pWriteConcern, 1);
		mongoc_write_concern_set_wmajority(pWriteConcern, 1000);
		mongoc_write_concern_append(pWriteConcern, optionBson);
		mongoc_read_concern_t* pReadConcern = mongoc_read_concern_new();
		mongoc_read_concern_set_level(pReadConcern, MONGOC_READ_CONCERN_LEVEL_LOCAL);
		mongoc_read_concern_append(pReadConcern, optionBson);
		//������ȡ�ļ����
		mongoc_read_prefs_t* pReadPrefs = mongoc_read_prefs_new(MONGOC_READ_PRIMARY);
		//�����洢Ͱ
		m_pGridfsBucket = mongoc_gridfs_bucket_new(m_pMongoDB, optionBson, pReadPrefs, &curError);
		//�ͷ��ڴ�
		mongoc_write_concern_destroy(pWriteConcern);
		mongoc_read_concern_destroy(pReadConcern);
		bson_destroy(optionBson);
	}
	else
	{
		return false;
	}
	return true;
}

void MongoManager::CleanConn()
{
	if (m_pGridfsBucket != NULL)
	{
		mongoc_gridfs_bucket_destroy(m_pGridfsBucket);
		m_pGridfsBucket = NULL;
	}
	if (m_pMongoCollection != NULL)
	{
		mongoc_collection_destroy(m_pMongoCollection);
		m_pMongoCollection = NULL;
	}
	if (m_pMongoDB != NULL)
	{
		mongoc_database_destroy(m_pMongoDB);
		m_pMongoDB = NULL;
	}
	if (m_pMongoClient != NULL)
	{
		mongoc_client_destroy(m_pMongoClient);
		m_pMongoClient = NULL;
	}
}

void MongoManager::InsertData(QString qstrInfo)
{
	bson_error_t curError;
	//����ÿ�ܷɻ�ÿ��������������,��������24Сʱ,��345600������
	//�˴���ֵ5000�Ǵӹٷ�ʾ��������ժ������,�������Է���һ���������,������������,������Ҳ�ǹٷ��Ƽ���һ���������
	for (int nLoopIndex = 0; nLoopIndex < 70; nLoopIndex++)
	{
		int nStart = nLoopIndex * 5000;
		int nEnd = nStart + 4999;
		bson_t bson[5000];
		bson_t* pBson[5000];
		double dLon = 73.5 + nLoopIndex * 5000 * 0.0000615;
		double dLat = 4.0 + nLoopIndex * 5000 * 0.000034;
		double dHeight = 0.0 + nLoopIndex * 5000 * 0.008;
		int64_t nTime = 1706460742000 + nLoopIndex * 5000 * 1000;
		for (int nIndex = 0; nIndex < 5000; nIndex++)
		{
			double dCurLon = dLon + nIndex * 0.0000615;
			double dCurLat = dLat + nIndex * 0.000034;
			double dCurHeight = dHeight + nIndex * 0.008;
			int64_t nCutTime = nTime + nIndex * 1000;
			bson_init(&bson[nIndex]);
			bson_append_int32(&bson[nIndex], "id", 2, m_nPlaneID);
			bson_append_int32(&bson[nIndex], "type", 4, m_nPlaneID);
			bson_append_int64(&bson[nIndex], "time", 4, nCutTime);
			bson_append_double(&bson[nIndex], "lon", 3, dCurLon);
			bson_append_double(&bson[nIndex], "lat", 3, dCurLat);
			bson_append_double(&bson[nIndex], "height", 6, dCurHeight);
			bson_append_double(&bson[nIndex], "roll", 4, 0.0);
			bson_append_double(&bson[nIndex], "pitch", 5, 0.0);
			bson_append_double(&bson[nIndex], "yaw", 3, 0.0);
			bson_append_double(&bson[nIndex], "north_speed", 11, 200.0);
			bson_append_double(&bson[nIndex], "up_speed", 8, 200.0);
			bson_append_double(&bson[nIndex], "east_speed", 10, 200.0);
			pBson[nIndex] = &bson[nIndex];
		}
		bool bSuccess = mongoc_collection_insert_many(m_pMongoCollection, (const bson_t**)pBson, 5000, NULL, NULL, &curError);
		if (bSuccess == false)
		{
			m_qstrInfo = m_qstrInfo + qstrInfo + QString::number(nStart) + QString::fromLocal8Bit("��") + QString::number(nEnd) + QString::fromLocal8Bit("������¼��ʧ��\n");
		}
		else
		{
			m_qstrInfo = m_qstrInfo + qstrInfo + QString::number(nStart) + QString::fromLocal8Bit("��") + QString::number(nEnd) + QString::fromLocal8Bit("������¼��ɹ�\n");
		}
		//�ͷ��ڴ�
		for (int nIndex = 0; nIndex < 5000; nIndex++)
		{
			bson_destroy(pBson[nIndex]);
		}
	}
}

void MongoManager::BulkOperation(QString qstrInfo)
{
	bson_error_t curError;
	//����ÿ�ܷɻ�ÿ��������������,��������24Сʱ,��345600������
	mongoc_bulk_operation_t* pBulk = mongoc_collection_create_bulk_operation_with_opts(m_pMongoCollection, NULL);
	if (pBulk == NULL)
	{
		m_qstrInfo = m_qstrInfo + qstrInfo + QString::fromLocal8Bit("����¼��ʧ��\n");
	}
	else
	{
		for (int nIndex = 0; nIndex < 350000; nIndex++)
		{
			bson_t* pDocBson = bson_new();
			double dLon = 73.5 + nIndex * 0.0000615;
			double dLat = 4.0 + nIndex * 0.000034;
			double dHeight = 0.0 + nIndex * 0.008;
			int64_t nTime = 1706460742000 + nIndex * 1000;
			bson_append_int32(pDocBson, "id", 2, m_nPlaneID);
			bson_append_int32(pDocBson, "type", 4, m_nPlaneID);
			bson_append_int64(pDocBson, "time", 4, nTime);
			bson_append_double(pDocBson, "lon", 3, dLon);
			bson_append_double(pDocBson, "lat", 3, dLat);
			bson_append_double(pDocBson, "height", 6, dHeight);
			bson_append_double(pDocBson, "roll", 4, 0.0);
			bson_append_double(pDocBson, "pitch", 5, 0.0);
			bson_append_double(pDocBson, "yaw", 3, 0.0);
			bson_append_double(pDocBson, "north_speed", 11, 200.0);
			bson_append_double(pDocBson, "up_speed", 8, 200.0);
			bson_append_double(pDocBson, "east_speed", 10, 200.0);
			QString qstrBlobKey = QString::fromLocal8Bit("data");
			QByteArray byteBlobKey = qstrBlobKey.toUtf8();
			const char* pBlobKey = byteBlobKey.constData();
			int nKeyLength = (int)strlen(pBlobKey);
			QByteArray dataByte;
			QDataStream blobStream(&dataByte, QIODevice::WriteOnly);
			blobStream << m_nPlaneID << m_nPlaneID << nTime << dLon << dLat << dHeight << 0.0 << 0.0 << 0.0 << 200.0 << 200.0 << 200.0;
			const char* pBlobValue = dataByte.constData();
			uint32_t nBinaryLength = dataByte.length();
			bson_append_binary(pDocBson, pBlobKey, nKeyLength, BSON_SUBTYPE_BINARY, (const uint8_t*)pBlobValue, nBinaryLength);
			mongoc_bulk_operation_insert(pBulk, pDocBson);
			bson_destroy(pDocBson);
		}
		bson_t reply;
		bool bSuccess = mongoc_bulk_operation_execute(pBulk, &reply, &curError);
		if (bSuccess == false)
		{
			m_qstrInfo = m_qstrInfo + qstrInfo + QString::fromLocal8Bit("����¼��ʧ��,ԭ����") + QString::fromLocal8Bit(curError.message) + QString::fromLocal8Bit("\n");
		}
		else
		{
			m_qstrInfo = m_qstrInfo + qstrInfo + QString::fromLocal8Bit("����¼��ɹ�\n");
		}
		//������������Ϣ
		char* pReplyInfo = bson_as_json(&reply, NULL);
		QString qstrReplyInfo = QString::fromUtf8(pReplyInfo);//�����������ݿ������Ϣ
		QJsonDocument curDoc = QJsonDocument::fromJson(qstrReplyInfo.toUtf8());
		if (curDoc.isObject() == true)
		{
			QJsonObject curObj = curDoc.object();
			if (curObj.contains(QString::fromLocal8Bit("nInserted")) == true)
			{
				int nInsertedCount = curObj["nInserted"].toInt();
				m_qstrInfo = m_qstrInfo + QString::number(nInsertedCount) + QString::fromLocal8Bit("������¼��ɹ�\n");
			}
			if (curObj.contains(QString::fromLocal8Bit("writeErrors")) == true)
			{
				if (curObj["writeErrors"].isArray() == true)
				{
					QJsonArray errorArray = curObj["writeErrors"].toArray();
					int nErrorCount = errorArray.size();
					for (int nErrorIndex = 0; nErrorIndex < nErrorCount; nErrorIndex++)
					{
						QJsonValue curValue = errorArray.at(nErrorIndex);
						if (curValue.isObject() == true)
						{
							QJsonObject curObj = curValue.toObject();
							//��ȡ������־��Ϣ
						}
					}
				}
			}
		}
		bson_destroy(&reply);
		mongoc_bulk_operation_destroy(pBulk);
	}
}

void MongoManager::slot_startTask()
{
	bool bConn = CreateConn();
	if (bConn == false)
	{
		emit sig_finishTask(false, m_nPlaneID, QString::fromLocal8Bit("���ݿ����Ӵ���ʧ��\n"));
		return;
	}
	bool bSuccess = false;
	if (m_nImportType == 0)
	{
		QString qstrInfo = QString::fromLocal8Bit("��ǰIDΪ") + QString::number(m_nPlaneID) + QString::fromLocal8Bit("�ɻ�:");
		//��������������ⷽʽ�������ԣ������ϲ�಻�󣬵��Ǹ��˻����Ƽ����õڶ�����ⷽʽ
		//��һ��������ⷽʽ
		//InsertData(qstrInfo);
		//�ڶ���������ⷽʽ
		BulkOperation(qstrInfo);
	}
	else if (m_nImportType == 1)
	{
		bson_error_t curError;
		QString qstrPicName = QString::fromLocal8Bit("dji_fly_20230920_113022_251_1695213545760_photo");
		QByteArray bytePicName = qstrPicName.toUtf8();
		const char* pPicName = bytePicName.constData();
		QString qstrPicPath = QApplication::applicationDirPath() + QString::fromLocal8Bit("/") + qstrPicName + QString::fromLocal8Bit(".jpg");
		QByteArray bytePicPath = qstrPicPath.toUtf8();
		const char* pPicPath = bytePicPath.constData();
		mongoc_stream_t* pPicStream = mongoc_stream_file_new_for_path(pPicPath, O_RDONLY, 0);
		if (pPicStream == NULL)
		{
			m_qstrInfo = m_qstrInfo + QString::fromLocal8Bit("ͼƬ¼��ʧ��\n");
		}
		else
		{
			bson_value_t file_id;
			bSuccess = mongoc_gridfs_bucket_upload_from_stream(m_pGridfsBucket, pPicName, pPicStream, NULL, &file_id, &curError);
			if (bSuccess == false)
			{
				m_qstrInfo = m_qstrInfo + QString::fromLocal8Bit("ͼƬ¼��ʧ��\n");
			}
			else
			{
				m_qstrInfo = m_qstrInfo + QString::fromLocal8Bit("ͼƬ¼��ɹ�\n");
			}
			//��ͼƬ����,��ԭͼƬ���бȶ�
			QString qstrDownloadPicPath = QApplication::applicationDirPath() + QString::fromLocal8Bit("/dji_fly_20230920_113022_251_1695213545760_photo-����.jpg");
			QByteArray byteDownloadPicPath = qstrDownloadPicPath.toLocal8Bit();
			const char* pDownloadPicPath = byteDownloadPicPath.constData();
			mongoc_stream_t* pDownloadPicStream = mongoc_stream_file_new_for_path(pDownloadPicPath, O_CREAT | O_RDWR, 0);
			if (pDownloadPicStream != NULL)
			{
				bool bDownload = mongoc_gridfs_bucket_download_to_stream(m_pGridfsBucket, &file_id, pDownloadPicStream, &curError);
				mongoc_stream_close(pDownloadPicStream);
				mongoc_stream_destroy(pDownloadPicStream);
			}
		}
		//�ͷ��ڴ�
		mongoc_stream_close(pPicStream);
		mongoc_stream_destroy(pPicStream);
	}
	else
	{
	}
	CleanConn();
	emit sig_finishTask(bSuccess, m_nPlaneID, m_qstrInfo);
}

void MongoManager::slot_startQuery()
{
	bool bConn = CreateConn();
	if (bConn == false)
	{
		emit sig_finishTask(false, QString::fromLocal8Bit("���ݿ����Ӵ���ʧ��\n"));
		return;
	}
	bson_t* pQueryBson = bson_new();
	bson_t orBson;
	BSON_APPEND_ARRAY_BEGIN(pQueryBson, "$or", &orBson);
	//ʱ����ڵ���1706460742000��С�ڵ���1706460751000��ѯ����
	bson_t conditionBson;
	BSON_APPEND_DOCUMENT_BEGIN(&orBson, "0", &conditionBson);
	bson_t greaterTimeBson;
	BSON_APPEND_DOCUMENT_BEGIN(&conditionBson, "time", &greaterTimeBson);
	BSON_APPEND_INT64(&greaterTimeBson, "$gte", 1706460742000);
	BSON_APPEND_INT64(&greaterTimeBson, "$lte", 1706460751000);
	bson_append_document_end(&conditionBson, &greaterTimeBson);
	bson_append_document_end(&orBson, &conditionBson);
	//���ȴ��ڵ���74.0��С�ڵ���74.0007��ѯ����
	BSON_APPEND_DOCUMENT_BEGIN(&orBson, "1", &conditionBson);
	bson_t greaterLonBson;
	BSON_APPEND_DOCUMENT_BEGIN(&conditionBson, "lon", &greaterLonBson);
	BSON_APPEND_DOUBLE(&greaterLonBson, "$gte", 74.0);
	BSON_APPEND_DOUBLE(&greaterLonBson, "$lte", 74.0007);
	bson_append_document_end(&conditionBson, &greaterLonBson);
	bson_append_document_end(&orBson, &conditionBson);
	bson_append_array_end(pQueryBson, &orBson);
	//�޶���ѯ���Ϊǰ15����¼
	bson_t* pOptionsBson = bson_new();
	BSON_APPEND_INT32(pOptionsBson, "limit", 15);
	//����ʱ�併������
	bson_t sortBson;
	bson_init(&sortBson);
	BSON_APPEND_INT32(&sortBson, "time", -1);
	BSON_APPEND_DOCUMENT(pOptionsBson, "sort", &sortBson);
	//ֻ��ѯʱ�䡢���Ⱥ������ֶ�
	bson_t prjBson;
	bson_init(&prjBson);
	BSON_APPEND_BOOL(&prjBson, "time", 1);
	BSON_APPEND_BOOL(&prjBson, "lon", 1);
	BSON_APPEND_BOOL(&prjBson, "data", 1);
	BSON_APPEND_DOCUMENT(pOptionsBson, "projection", &prjBson);
	mongoc_cursor_t* pCursor = mongoc_collection_find_with_opts(m_pMongoCollection, pQueryBson, pOptionsBson, NULL);
	int nRecordIndex = 0;
	const bson_t* pResultDoc;
	while (mongoc_cursor_next(pCursor, &pResultDoc) == true)
	{
		nRecordIndex++;
		m_qstrInfo = m_qstrInfo + QString::fromLocal8Bit("��") + QString::number(nRecordIndex) + QString::fromLocal8Bit("�����ݼ�¼\n");
		bson_iter_t curIter;
		bool bInit = bson_iter_init(&curIter, pResultDoc);
		if (bInit == true)
		{
			while (bson_iter_next(&curIter) == true)
			{
				const char* pKey = bson_iter_key(&curIter);//��ȡ����
				QString qstrKey = QString::fromUtf8(pKey);
				if (BSON_ITER_HOLDS_BINARY(&curIter) == true)
				{
					uint32_t nBinaryLength;
					const uint8_t* pBlobValue;
					bson_iter_binary(&curIter, NULL, &nBinaryLength, &pBlobValue);
					const char* pRawData = (const char*)pBlobValue;
					QByteArray dataByte = QByteArray::fromRawData(pRawData, nBinaryLength);
					QDataStream dataStream(&dataByte, QIODevice::ReadOnly);
					int nPlaneID = 0;
					dataStream >> nPlaneID;
					int nPlaneType = 0;
					dataStream >> nPlaneType;
					int64_t nTime = 0;
					dataStream >> nTime;
					double dLon = -9999.0;
					dataStream >> dLon;
					double dLat = -9999.0;
					dataStream >> dLat;
					double dHeight = -9999.0;
					dataStream >> dHeight;
					double dRoll = -9999.0;
					dataStream >> dRoll;
					double dPitch = -9999.0;
					dataStream >> dPitch;
					double dYaw = -9999.0;
					dataStream >> dYaw;
					double dNorthSpeed = -9999.0;
					dataStream >> dNorthSpeed;
					double dUpSpeed = -9999.0;
					dataStream >> dUpSpeed;
					double dEastSpeed = -9999.0;
					dataStream >> dEastSpeed;
					m_qstrInfo = m_qstrInfo + qstrKey + QString::fromLocal8Bit(":") 
						+ QString::number(nPlaneID) + QString::fromLocal8Bit("__") 
						+ QString::number(nPlaneType) + QString::fromLocal8Bit("__")
						+ QString::number(nTime) + QString::fromLocal8Bit("__")
						+ QString::number(dLon, 'g', 10) + QString::fromLocal8Bit("__")
						+ QString::number(dLat, 'g', 10) + QString::fromLocal8Bit("__")
						+ QString::number(dHeight, 'g', 10) + QString::fromLocal8Bit("__")
						+ QString::number(dRoll, 'g', 10) + QString::fromLocal8Bit("__")
						+ QString::number(dPitch, 'g', 10) + QString::fromLocal8Bit("__")
						+ QString::number(dYaw, 'g', 10) + QString::fromLocal8Bit("__")
						+ QString::number(dNorthSpeed, 'g', 10) + QString::fromLocal8Bit("__")
						+ QString::number(dUpSpeed, 'g', 10) + QString::fromLocal8Bit("__")
						+ QString::number(dEastSpeed, 'g', 10) + QString::fromLocal8Bit("\n");
				}
				else if (BSON_ITER_HOLDS_INT32(&curIter) == true)
				{
					int32_t nValue = bson_iter_int32(&curIter);
					m_qstrInfo = m_qstrInfo + qstrKey + QString::fromLocal8Bit(":") + QString::number(nValue) + QString::fromLocal8Bit("\n");
				}
				else if (BSON_ITER_HOLDS_INT64(&curIter) == true)
				{
					int64_t nValue = bson_iter_int64(&curIter);
					m_qstrInfo = m_qstrInfo + qstrKey + QString::fromLocal8Bit(":") + QString::number(nValue) + QString::fromLocal8Bit("\n");
				}
				else if (BSON_ITER_HOLDS_DOUBLE(&curIter) == true)
				{
					double dValue = bson_iter_double(&curIter);
					m_qstrInfo = m_qstrInfo + qstrKey + QString::fromLocal8Bit(":") + QString::number(dValue, 'g', 10) + QString::fromLocal8Bit("\n");
				}
			}
		}
	}
	bson_destroy(pOptionsBson);
	bson_destroy(pQueryBson);
	mongoc_cursor_destroy(pCursor);
	CleanConn();
	emit sig_finishTask(true, m_qstrInfo);
}
