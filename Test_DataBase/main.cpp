#include "Test_DataBase.h"
#include <QtWidgets/QApplication>

int main(int argc, char *argv[])
{
    QApplication a(argc, argv);
    Test_DataBase w;
    w.show();
    return a.exec();
}
