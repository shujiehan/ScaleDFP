#!bin/bash
# dump pickle
# 2: load, 3: save
START_DATE="20180103"
DATE_FORMAT="%Y%m%d"
DISK_MODEL="MC1"
DATA_PATH="~/trace/alibaba_ssd/"
TRAIN_PATH="./mc1_train_p30n7v30_fix_20/"
TEST_PATH="./mc1_test_p30n7v30_fix_20/"
ITER_DAYS=10
PATH_FEATUERS="features_erg/mc1_all.txt"
OPTIONS="3,4,6"
FORGET_TYPE="sliding"
LABEL_DAYS=20
POSITIVE_WINDOW=30
NEGATIVE_WINDOW=7
VALIDATION_WINDOW=30

REPORT_NAME="mc1_p30n7v30_fix_20"
SAVE_DIR="save_model/"
PATH_LOAD="${SAVE_DIR}${REPORT_NAME}.pickle"
PATH_SAVE="${SAVE_DIR}${REPORT_NAME}.pickle"
TIME_PATH="time_${REPORT_NAME}.txt"

if [ ! -d ${TRAIN_PATH} ]; then
mkdir $TRAIN_PATH
mkdir $TEST_PATH
fi

if [ ! -d ${SAVE_DIR} ]; then
mkdir $SAVE_DIR
fi

echo "python run.py -s $START_DATE -F $DATE_FORMAT -p $DATA_PATH -d $DISK_MODEL -i $ITER_DAYS \
  -l $PATH_LOAD -v $PATH_SAVE -c $PATH_FEATUERS -r $TRAIN_PATH -e $TEST_PATH -o $OPTIONS \
  -t $FORGET_TYPE -w $POSITIVE_WINDOW -V $VALIDATION_WINDOW -L $NEGATIVE_WINDOW \
  -a $LABEL_DAYS"

echo "python run.py -s $START_DATE -F $DATE_FORMAT -p $DATA_PATH -d $DISK_MODEL -i $ITER_DAYS \
  -l $PATH_LOAD -v $PATH_SAVE -c $PATH_FEATUERS -r $TRAIN_PATH -e $TEST_PATH -o $OPTIONS \
  -t $FORGET_TYPE -w $POSITIVE_WINDOW -V $VALIDATION_WINDOW -L $NEGATIVE_WINDOW \
  -a $LABEL_DAYS" >> $TIME_PATH

time (python run.py -s $START_DATE -F $DATE_FORMAT -p $DATA_PATH -d "$DISK_MODEL" -i $ITER_DAYS \
  -l $PATH_LOAD -v $PATH_SAVE -c $PATH_FEATUERS -r $TRAIN_PATH -e $TEST_PATH -o $OPTIONS \
  -t $FORGET_TYPE -w $POSITIVE_WINDOW -V $VALIDATION_WINDOW -L $NEGATIVE_WINDOW \
  -a $LABEL_DAYS) 2>> $TIME_PATH
