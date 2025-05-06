#!/bin/bash
#wrote by liujun693 at 2019.10.29
if [ $# != 1 ] ; then
echo "USAGE: $0 ProjectPath"
echo " e.g.: $0 /wls/applications/sqlreview/"
exit 1;
fi
ProjectPath=$1
ProjectComplie=${ProjectPath}.compile
echo "********************initializ..."
rm -rf ${ProjectComplie}
echo "********************initialized"
echo "********************compiling......"
cp -r ${ProjectPath} ${ProjectComplie}
/wls/miniconda3/envs/sqlreview/bin/python3 -O -m compileall -b ${ProjectComplie} #这是编译的关键
echo "#############"
echo "${ProjectComplie}"
if [ $? -ne 0 ] ; then
echo -e "**************************************************"
echo "* python error ,please use virtualenv or python *"
echo -e "**************************************************\n"
exit $?
fi
echo "********************compiled"
echo "!!!!!!!!!!!!!!!!!!!!deleting files......"
find ${ProjectComplie}/ -name "*.py"|xargs rm -rf
find ${ProjectComplie}/ -name ".git*"|xargs rm -rf
find ${ProjectComplie}/ -name ".idea*"|xargs rm -rf
find ${ProjectComplie} -name "__pycache__"|xargs rm -rf
find ${ProjectComplie} -name ".gitkeep"|xargs rm -rf
find ${ProjectComplie} -name "README.md"|xargs rm -rf
#find ${ProjectComplie}/luflow -name "*.py"|grep -v 'settings.py\|wsgi.py'|xargs rm -rf
echo "!!!!!!!!!!!!!!!!!!!!deleted"
echo "done!!!"
