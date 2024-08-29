#include "Test_DataBase.h"
#include "MongoManager.h"
#include "MySQLManager.h"

#include <QDateTime>
#include <QFile>
#include <QDomDocument>

using namespace std;

Test_DataBase::Test_DataBase(QWidget *parent)
    : QWidget(parent)
{
    ui.setupUi(this);
    CreateConn();
    QString qstrConfigPath = QApplication::applicationDirPath() + QString::fromLocal8Bit("/DatabaseConfig.xml");
    ParseDBConfig(qstrConfigPath);
    m_nMongoStartTime = 0;
    m_nMongoEndTime = 0;
    m_nMongoFinishCount = 0;
	m_nMySQLStartTime = 0;
	m_nMySQLEndTime = 0;
	m_nMySQLFinishCount = 0;
	//初始化Mongo数据库
	mongoc_init();
    //初始化MySQL数据库驱动
    m_pDriver = get_driver_instance();
}

Test_DataBase::~Test_DataBase()
{
	//释放数据库内存
	mongoc_cleanup();
}

void Test_DataBase::CreateConn()
{
	connect(ui.pMongoDocBtn, SIGNAL(clicked()), this, SLOT(slot_MongoDoc()));
	connect(ui.pMongoUploadPicBtn, SIGNAL(clicked()), this, SLOT(slot_MongoUploadPic()));
	connect(ui.pMongoQueryDocBtn, SIGNAL(clicked()), this, SLOT(slot_MongoQueryDoc()));
	connect(ui.pInsertMySQLBtn, SIGNAL(clicked()), this, SLOT(slot_insertMySQL()));
}

void Test_DataBase::ParseDBConfig(QString qstrConfigPath)
{
    QFile curConfigFile(qstrConfigPath);
    bool bOpen = curConfigFile.open(QFile::ReadOnly);
    if (bOpen == true)
    {
        QDomDocument curDoc;
        bool bRead = curDoc.setContent(&curConfigFile);
        if (bRead == false)
        {
            curConfigFile.close();
            return;
        }
        QDomElement pRootEle = curDoc.documentElement();
        QString qstrRootNodeName = pRootEle.nodeName();
        if (qstrRootNodeName == QString::fromLocal8Bit("Database"))
        {
            QDomNode childNode = pRootEle.firstChild();
            while (childNode.isNull() == false)
            {
                if (childNode.isElement() == true)
                {
                    QDomElement curEle = childNode.toElement();
                    QString qstrType = curEle.attribute("type");
                    if (qstrType == QString::fromLocal8Bit("MongoDB"))
                    {
                        QDomNodeList curNodeList = curEle.childNodes();
                        int nNodeCount = curNodeList.count();
                        for (int nNodeIndex = 0; nNodeIndex < nNodeCount; nNodeIndex++)
                        {
                            QDomNode curNode = curNodeList.at(nNodeIndex);
                            if (curNode.isElement() == true)
                            {
                                QDomElement curNodeEle = curNode.toElement();
                                QString qstrTagName = curNodeEle.tagName();
                                if (qstrTagName == QString::fromLocal8Bit("URI"))
                                {
                                    m_qstrMongoURI = curNodeEle.text();
                                }
                                else if (qstrTagName == QString::fromLocal8Bit("Name"))
                                {
                                    m_qstrMongoDatabaseName = curNodeEle.text();
                                }
								else if (qstrTagName == QString::fromLocal8Bit("UserName"))
								{
                                    m_qstrMongoUserName = curNodeEle.text();
								}
								else if (qstrTagName == QString::fromLocal8Bit("Password"))
								{
                                    m_qstrMongoPassword = curNodeEle.text();
								}
								else
								{

								}
                            }
                        }
                    }
					else if (qstrType == QString::fromLocal8Bit("MySQL"))
					{
						QDomNodeList curNodeList = curEle.childNodes();
						int nNodeCount = curNodeList.count();
						for (int nNodeIndex = 0; nNodeIndex < nNodeCount; nNodeIndex++)
						{
							QDomNode curNode = curNodeList.at(nNodeIndex);
							if (curNode.isElement() == true)
							{
								QDomElement curNodeEle = curNode.toElement();
								QString qstrTagName = curNodeEle.tagName();
								if (qstrTagName == QString::fromLocal8Bit("HostName"))
								{
                                    m_qstrMySQLHostName = curNodeEle.text();
								}
								else if (qstrTagName == QString::fromLocal8Bit("UserName"))
								{
									m_qstrMySQLUserName = curNodeEle.text();
								}
								else if (qstrTagName == QString::fromLocal8Bit("Password"))
								{
									m_qstrMySQLPassword = curNodeEle.text();
								}
								else
								{

								}
							}
						}
					}
                    else
                    {

                    }
				}
                childNode = childNode.nextSibling();
            }
        }
        curConfigFile.close();
    }
}

void Test_DataBase::slot_MongoDoc()
{
    //假设此时空中共有3架飞机
    m_nMongoStartTime = QDateTime::currentMSecsSinceEpoch();
    for (int nPlaneIndex = 0; nPlaneIndex < 3; nPlaneIndex++)
    {
        int nPlaneID = nPlaneIndex + 1;
        MongoManager* pMongoManager = new MongoManager(m_qstrMongoURI, m_qstrMongoDatabaseName, nPlaneID, NULL);
		connect(pMongoManager, SIGNAL(sig_finishTask(bool, int, QString)), this, SLOT(slot_finishMongoTask(bool, int, QString)));
        pMongoManager->Start();
    }
}

void Test_DataBase::slot_MongoUploadPic()
{
    m_nMongoStartTime = QDateTime::currentMSecsSinceEpoch();
	MongoManager* pMongoManager = new MongoManager(m_qstrMongoURI, m_qstrMongoDatabaseName, NULL);
	connect(pMongoManager, SIGNAL(sig_finishTask(bool, int, QString)), this, SLOT(slot_finishMongoTask(bool, int, QString)));
    pMongoManager->SetImportType(1);
	pMongoManager->Start();
}

void Test_DataBase::slot_MongoQueryDoc()
{
    MongoManager* pMongoManager = new MongoManager(m_qstrMongoURI, m_qstrMongoDatabaseName, 1, NULL);
	connect(pMongoManager, SIGNAL(sig_finishTask(bool, QString)), this, SLOT(slot_finishMongoQueryTask(bool, QString)));
	pMongoManager->Query();
}

void Test_DataBase::slot_finishMongoTask(bool bSuccess, int nPlaneID, QString qstrInfo)
{
    QString qstrLog = ui.pMongoEdt->toPlainText() + qstrInfo;
    ui.pMongoEdt->setPlainText(qstrLog);
    m_nMongoFinishCount++;
    if (m_nMongoFinishCount == 3)
    {
        m_nMongoEndTime = QDateTime::currentMSecsSinceEpoch();
        qint64 nTakeTime = m_nMongoEndTime - m_nMongoStartTime;
        qstrLog = ui.pMongoEdt->toPlainText() + QString::fromLocal8Bit("录入数据共耗时:") + QString::number(nTakeTime) + QString::fromLocal8Bit("毫秒");
        ui.pMongoEdt->setPlainText(qstrLog);
        m_nMongoFinishCount = 0;
        m_nMongoStartTime = 0;
        m_nMongoEndTime = 0;
    }
}

void Test_DataBase::slot_finishMongoQueryTask(bool bSuccess, QString qstrInfo)
{
	QString qstrLog = ui.pMongoEdt->toPlainText() + qstrInfo;
	ui.pMongoEdt->setPlainText(qstrLog);
}

void Test_DataBase::slot_insertMySQL()
{
	//假设此时空中共有3架飞机
	m_nMySQLStartTime = QDateTime::currentMSecsSinceEpoch();
	for (int nPlaneIndex = 0; nPlaneIndex < 1; nPlaneIndex++)
	{
		int nPlaneID = nPlaneIndex + 1;
        MySQLManager* pMySQLManager = new MySQLManager(m_pDriver, m_qstrMySQLHostName, m_qstrMySQLUserName, m_qstrMySQLPassword, nPlaneID, NULL);
		connect(pMySQLManager, SIGNAL(sig_finishTask(bool, int, QString)), this, SLOT(slot_finishMySQLTask(bool, int, QString)));
        pMySQLManager->Start();
	}
}

void Test_DataBase::slot_finishMySQLTask(bool bSuccess, int nPlaneID, QString qstrInfo)
{
	QString qstrLog = ui.pMySQLEdt->toPlainText() + qstrInfo;
	ui.pMySQLEdt->setPlainText(qstrLog);
    m_nMySQLFinishCount++;
    if (m_nMySQLFinishCount == 3)
    {
		m_nMySQLEndTime = QDateTime::currentMSecsSinceEpoch();
		qint64 nTakeTime = m_nMySQLEndTime - m_nMySQLStartTime;
		qstrLog = ui.pMySQLEdt->toPlainText() + QString::fromLocal8Bit("录入数据共耗时:") + QString::number(nTakeTime) + QString::fromLocal8Bit("毫秒");
		ui.pMySQLEdt->setPlainText(qstrLog);
    }
}
