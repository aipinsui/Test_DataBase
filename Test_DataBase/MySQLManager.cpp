#include "MySQLManager.h"

#include "jdbc/cppconn/statement.h"

#include <QApplication>
#include <QFile>
#include <QTextStream>

MySQLManager::MySQLManager(sql::Driver* pDriver, QString qstrHostName, QString qstrUserName, QString qstrPassword, int nPlaneID, QObject *parent)
	: QObject(parent)
{
	m_pDriver = pDriver;
	m_qstrHostName = qstrHostName;
	m_qstrUserName = qstrUserName;
	m_qstrPassword = qstrPassword;
	m_nPlaneID = nPlaneID;
}

MySQLManager::~MySQLManager()
{

}

void MySQLManager::Start()
{
	//创建任务线程
	m_pThread = new QThread();
	connect(this, SIGNAL(sig_startThread()), this, SLOT(slot_startTask()));
	this->moveToThread(m_pThread);
	m_pThread->start();
	emit sig_startThread();
}

bool MySQLManager::createConn()
{
	if (m_pDriver == NULL)
	{
		return false;
	}
	//连接数据库
	sql::ConnectOptionsMap connection_properties;
	connection_properties["hostName"] = m_qstrHostName.toStdString();
	connection_properties["userName"] = m_qstrUserName.toStdString();
	connection_properties["password"] = m_qstrPassword.toStdString();
	connection_properties["CLIENT_MULTI_STATEMENTS"] = true;
	m_pConn = m_pDriver->connect(connection_properties);//建立连接
	if (m_pConn == NULL)
	{
		return false;
	}
	return true;
}

void MySQLManager::cleanConn()
{
	if (m_pConn != NULL)
	{
		m_pConn->close();
		if (m_pConn->isClosed())
		{
			delete m_pConn;
			m_pConn = NULL;
		}
	}
}

void MySQLManager::slot_startTask()
{
	bool bConn = createConn();
	if (bConn == false)
	{
		emit sig_finishTask(false, m_nPlaneID, QString::fromLocal8Bit("数据库连接创建失败\n"));
		return;
	}
	bool bSuccess = false;
	QString qstrInfo = QString::fromLocal8Bit("当前ID为") + QString::number(m_nPlaneID) + QString::fromLocal8Bit("飞机:");
	QString qstrTableName = QString::fromLocal8Bit("aircraft_info_") + QString::number(m_nPlaneID);
	//假设每架飞机每秒四条飞行数据，连续飞行24小时，共345600条数据
	sql::Statement* pStatement = m_pConn->createStatement();
	if (pStatement == NULL)
	{
		emit sig_finishTask(false, m_nPlaneID, QString::fromLocal8Bit("数据库事务创建失败\n"));
		return;
	}
	//输出数据文件
	QString qstrDataPath = QApplication::applicationDirPath() + QString::fromLocal8Bit("/")
		+ qstrTableName + QString::fromLocal8Bit(".txt");
	QFile dataFile(qstrDataPath);
	bool bOpen = dataFile.open(QIODevice::ReadWrite | QIODevice::Text);
	QTextStream dataStream(&dataFile);
	for (int nLoopIndex = 0; nLoopIndex < 70; nLoopIndex++)
	{
		double dLon = 73.5 + nLoopIndex * 5000 * 0.0000615;
		double dLat = 4.0 + nLoopIndex * 5000 * 0.000034;
		double dHeight = 0.0 + nLoopIndex * 5000 * 0.008;
		int64_t nTime = 1706460742000 + nLoopIndex * 5000 * 1000;
		for (int nIndex = 0; nIndex < 5000; nIndex++)
		{
			int nID = nLoopIndex * 5000 + nIndex + 1;
			double dCurLon = dLon + nIndex * 0.0000615;
			double dCurLat = dLat + nIndex * 0.000034;
			double dCurHeight = dHeight + nIndex * 0.008;
			int64_t nCutTime = nTime + nIndex * 1000;
			if (nLoopIndex == 69 && nIndex == 4999)
			{
				dataStream << nID << "," << m_nPlaneID << "," << m_nPlaneID << "," << nCutTime << ","
					<< dCurLon << "," << dCurLat << "," << dCurHeight << ","
					<< 0.0 << "," << 0.0 << "," << 0.0 << ","
					<< 200.0 << "," << 200.0 << "," << 200.0;
			}
			else
			{
				dataStream << nID << "," << m_nPlaneID << "," << m_nPlaneID << "," << nCutTime << ","
					<< dCurLon << "," << dCurLat << "," << dCurHeight << ","
					<< 0.0 << "," << 0.0 << "," << 0.0 << ","
					<< 200.0 << "," << 200.0 << "," << 200.0 << endl;
			}
		}
	}
	dataFile.close();
	//录入数据
	QString qstrSQL = QString::fromLocal8Bit("load data infile '") + qstrDataPath.replace("/","\\\\") + QString::fromLocal8Bit("' replace into table ") + qstrTableName
		+ QString::fromLocal8Bit(" character set utf8mb4 fields terminated by ',' optionally enclosed by '\"' lines terminated by '\n'");
	std::string strSQL = qstrSQL.toStdString();
	bSuccess = pStatement->execute(sql::SQLString(strSQL.c_str()));//MySQL数据库执行SELECT语句才会返回true
	uint64_t nUpdateCount = pStatement->getUpdateCount();
	m_qstrInfo = qstrInfo + QString::number(nUpdateCount) + QString::fromLocal8Bit("条数据录入成功\n");
	if (pStatement != NULL)
	{
		pStatement->close();
		delete pStatement;
		pStatement = NULL;
	}
	cleanConn();
	emit sig_finishTask(bSuccess, m_nPlaneID, m_qstrInfo);
}
