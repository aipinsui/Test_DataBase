#pragma once

#include <QtWidgets/QWidget>
#include "ui_Test_DataBase.h"

#include "jdbc/cppconn/driver.h"

class Test_DataBase : public QWidget
{
    Q_OBJECT

public:
    Test_DataBase(QWidget *parent = Q_NULLPTR);
    ~Test_DataBase();

private:
	void CreateConn();
    //解析数据库连接相关配置
    void ParseDBConfig(QString qstrConfigPath);

private slots:
    //存储文档数据
	void slot_MongoDoc();
    //上传图片数据
	void slot_MongoUploadPic();
	//查询文档数据
	void slot_MongoQueryDoc();

    void slot_finishMongoTask(bool bSuccess, int nPlaneID, QString qstrInfo);

	void slot_finishMongoQueryTask(bool bSuccess, QString qstrInfo);

	void slot_insertMySQL();

    void slot_finishMySQLTask(bool bSuccess, int nPlaneID, QString qstrInfo);

private:
    qint64 m_nMongoStartTime;

	qint64 m_nMongoEndTime;

    int m_nMongoFinishCount;

	qint64 m_nMySQLStartTime;

	qint64 m_nMySQLEndTime;

	int m_nMySQLFinishCount;

    sql::Driver* m_pDriver;

    QString m_qstrMongoURI;//MongoDB数据库服务器地址

    QString m_qstrMongoDatabaseName;//MongoDB数据库名称

	QString m_qstrMongoUserName;

	QString m_qstrMongoPassword;

	QString m_qstrMySQLHostName;//MySQL数据库服务器地址

	QString m_qstrMySQLUserName;//MySQL数据库用户名

	QString m_qstrMySQLPassword;//MySQL数据库密码

private:
    Ui::Test_DataBaseClass ui;
};
