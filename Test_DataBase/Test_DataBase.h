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
    //�������ݿ������������
    void ParseDBConfig(QString qstrConfigPath);

private slots:
    //�洢�ĵ�����
	void slot_MongoDoc();
    //�ϴ�ͼƬ����
	void slot_MongoUploadPic();
	//��ѯ�ĵ�����
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

    QString m_qstrMongoURI;//MongoDB���ݿ��������ַ

    QString m_qstrMongoDatabaseName;//MongoDB���ݿ�����

	QString m_qstrMongoUserName;

	QString m_qstrMongoPassword;

	QString m_qstrMySQLHostName;//MySQL���ݿ��������ַ

	QString m_qstrMySQLUserName;//MySQL���ݿ��û���

	QString m_qstrMySQLPassword;//MySQL���ݿ�����

private:
    Ui::Test_DataBaseClass ui;
};
