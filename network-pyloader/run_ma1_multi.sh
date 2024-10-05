#!/bin/bash
# dump pickle
# 2: load, 3: save
PEER=$1
COLLECTOR="collector$2"
START_DATE="20180103"
DATE_FORMAT="%Y%m%d"
DISK_MODEL="MA1"
DATA_PATH="/home/shujie/ali_raw_${PEER}p/${COLLECTOR}/"
FRAC=0.1
TEST="accuracy"

DOWN_RATIO=7
LAMBDA_P=6
LAMBDA_N=1
#SEED=$((ID * 1000))
SEED=1

TRAIN_PATH="/home/shujie/ali_pyloader_${PEER}p_random${FRAC}/a3_train_p30n7v30_fix_20/allocate_d${DOWN_RATIO}p${LAMBDA_P}n${LAMBDA_N}r${SEED}/"
TEST_PATH="/home/shujie/ali_pyloader_${PEER}p_random${FRAC}/a3_test_p30n7v30_fix_20/allocate/"
ITER_DAYS=400
PATH_FEATUERS="features_erg/ma1_all.txt"
OPTIONS="3,4,6,7"
FORGET_TYPE="sliding"
LABEL_DAYS=20
POSITIVE_WINDOW=30
NEGATIVE_WINDOW=7
VALIDATION_WINDOW=30

LAMBDA_P=6
LAMBDA_N=1
SEED=1

REPORT_NAME="a3_p30n7v30_fix_20"
SAVE_DIR="save_model/"
PATH_LOAD="${SAVE_DIR}${LOAD_NAME}.pickle"
PATH_SAVE="${SAVE_DIR}${REPORT_NAME}.pickle"
TIME_PATH="time_${REPORT_NAME}.txt"

#if [ ! -d ${TRAIN_PATH} ]; then
#mkdir -p $TRAIN_PATH
#mkdir -p $TEST_PATH
#fi
#
#if [ ! -d ${SAVE_DIR} ]; then
#mkdir $SAVE_DIR
#fi

echo "python run_random_down.py -s $START_DATE -p $DATA_PATH -d $DISK_MODEL -i $ITER_DAYS \
  -v $PATH_SAVE -c $PATH_FEATUERS -r $TRAIN_PATH -e $TEST_PATH -o $OPTIONS \
  -t $FORGET_TYPE -w $POSITIVE_WINDOW -V $VALIDATION_WINDOW -L $NEGATIVE_WINDOW \
  -a $LABEL_DAYS -C $COLLECTOR -x $FRAC -R $SEED -D $DOWN_RATIO -P $LAMBDA_P -N $LAMBDA_N -T $TEST"

echo "python run_random_down.py -s $START_DATE -p $DATA_PATH -d $DISK_MODEL -i $ITER_DAYS \
  -v $PATH_SAVE -c $PATH_FEATUERS -r $TRAIN_PATH -e $TEST_PATH -o $OPTIONS \
  -t $FORGET_TYPE -w $POSITIVE_WINDOW -V $VALIDATION_WINDOW -L $NEGATIVE_WINDOW \
  -a $LABEL_DAYS -C $COLLECTOR -x $FRAC -R $SEED -D $DOWN_RATIO -P $LAMBDA_P -N $LAMBDA_N -T $TEST" >> $TIME_PATH

time (python run_random_down.py -s $START_DATE -p $DATA_PATH -d "$DISK_MODEL" -i $ITER_DAYS \
  -v $PATH_SAVE -c $PATH_FEATUERS -r $TRAIN_PATH -e $TEST_PATH -o $OPTIONS \
  -t $FORGET_TYPE -w $POSITIVE_WINDOW -V $VALIDATION_WINDOW -L $NEGATIVE_WINDOW \
  -a $LABEL_DAYS -C $COLLECTOR -x $FRAC -R $SEED -D $DOWN_RATIO -P $LAMBDA_P -N $LAMBDA_N -T $TEST) 2>> $TIME_PATH
