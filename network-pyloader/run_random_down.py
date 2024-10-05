import sys
import datetime
import getopt
import time
import pickle
from core_utils.abstract_predict import AbstractPredict
from simple_collector import Client
from utils.memory import Memory
from utils.arff import Arff
from utils.allocate import Allocate
import config
import socket
import re


class Simulate(AbstractPredict):
    def __init__(self, path, date_format, start_date, positive_window_size,  #manufacturer, \
                 disk_model, columns, features, label, forget_type, bl_delay=False, \
                 dropna=False, negative_window_size=6, validation_window=6, \
                 bl_regression=False, label_days=None, bl_transfer=False, bl_ssd=False,
                 file_format="arff", collector=None, random_seed=0, num_trees=10, down_ratio=5, lamb_p=6, lamb_n=1,
                 frac=None, bl_allocation=False):
        super().__init__()
        self.memory = Memory(path, start_date, positive_window_size,  #manufacturer,\
                             disk_model, columns, features, label, forget_type, dropna, bl_delay, \
                             negative_window_size, bl_regression, label_days, bl_transfer, date_format, bl_ssd)
        if not bl_transfer:
            self.memory.buffering()
            #self.data = self.memory.ret_df.drop(['model', 'date'], axis=1)
            self.data = self.memory.ret_df.drop(['model'], axis=1)
            self.data['date'] = self.data['date'].dt.date
            #self.data = self.data.with_columns(pl.col(['date']).cast(pl.Date))
        else:
            self.data = self.memory.ret_df.drop(['model', 'date'], axis=1)
            self.data['date'] = self.data['date'].dt.date
        self.data = self.data.reset_index(drop=True)
        self.class_name = label[0]
        self.num_classes = 2
        self.bl_delay = bl_delay
        self.validation_window = validation_window

        self.bl_regression = bl_regression
        self.bl_transfer = bl_transfer
        self.file_format = file_format
        self.collector = collector
        self.num_trees = num_trees
        self.bl_allocation = bl_allocation
        #self.random_list = [Random(random_seed + idx) for idx in range(self.num_trees)]
        self.allocate = Allocate(features=features,
                                 collector=self.collector,
                                 random_seed=random_seed,
                                 frac=frac,
                                 bl_allocation=bl_allocation,
                                 num_trees=num_trees,
                                 down_ratio=down_ratio,
                                 lamb_p=lamb_p,
                                 lamb_n=lamb_n)
        if self.file_format == "arff":
            self.arff = Arff(bl_regression=bl_regression)

    def load(self):
        # Load Data from Memory class and backtracking delayed instances
        self.memory.data_management(self.keep_delay, self.bl_delay)

        #self.data = self.memory.ret_df.drop(['model', 'date'], axis=1)
        self.data = self.memory.ret_df.drop(['model'], axis=1)
        self.data['date'] = self.data['date'].dt.date

        #self.data = self.memory.ret_df.drop(['model'])
        #self.data = self.data.with_columns(pl.col(['date']).cast(pl.Date))
        self.data = self.data.reset_index(drop=True)

    def delay_evaluate(self):
        pop_sn = []
        i = 0
        for sn, instances in self.keep_delay.items():
            instances.dequeue()
            if len(instances.queue) == 0:
                pop_sn.append(sn)
            i += 1
        for sn in pop_sn:
            self.keep_delay.pop(sn)

    def run(self):
        self.inspect(self.data, self.class_name, self.num_classes,
                     self.memory.new_inst_start_index, self.validation_window)

    def write_train_data(self, train_path, fname):
        if self.file_format == "arff":
            if not self.bl_regression:
                if 0 in self.data['failure'].values:
                    self.data['failure'] = self.data['failure'].map({
                        0: 'c0',
                        1: 'c1'
                    })
            self.allocate.send_downsampling_random(fname, self.data, train_path + fname + ".arff")
            #self.arff.dump(fname, self.data, train_path + fname + ".arff")

        elif self.file_format == "csv":
            self.data.to_csv(train_path + fname + ".csv", index=False)

    def write_test_data(self, test_path, fname):
        if not self.bl_transfer:
            if test_path is not None and self.memory.new_inst_start_index > 0:
                if self.file_format == "arff":
                    if 0 in self.data['failure'].values:
                        self.data['failure'] = self.data['failure'].map({
                            0: 'c0',
                            1: 'c1'
                        })
                    self.arff.dump(fname,
                                   self.data[self.memory.new_inst_start_index:],
                                   test_path + fname + ".arff")
                elif self.file_format == "csv":
                    self.data[self.memory.new_inst_start_index:].to_csv(
                        test_path + fname + ".csv", index=False)
        else:
            if test_path is not None:
                if self.file_format == "arff":
                    if not self.bl_regression:
                        self.data['failure'] = self.data['failure'].map({
                            0: 'c0',
                            1: 'c1'
                        })
                    self.arff.dump(fname,
                                   self.data[self.memory.new_inst_start_index:],
                                   test_path + fname + ".arff")
                elif self.file_format == "csv":
                    self.data[self.memory.new_inst_start_index:].to_csv(
                        test_path + fname + ".csv", index=False)


def run_simulating(start_date, path, path_load, path_save, train_path,
                   test_path, file_format, iter_days, model, features, label,
                   columns, forget_type, positive_window_size, bl_delay,
                   bl_load, bl_save, negative_window_size, validation_window,
                   bl_regression, label_days, bl_transfer, bl_ssd, date_format,
                   client_id, random_seed, num_trees, down_ratio,
                   lamb_p, lamb_n, frac, bl_allocation, test):
    collector = Client(client_name=client_id, coord_addr=config.COORD_ADDR, 
                       receivers_addr_map=config.RECEIVER_ADDR_MAP,
                       test=test)

    if bl_load:
        with open(path_load, 'rb') as f:
            sim = pickle.load(f)
        #print(sim.memory.cur_date)
        date = sim.memory.cur_date
        #sim.load()
    else:
        print(start_date)
        sim = Simulate(path, date_format, start_date, positive_window_size, model, columns,
                       features, label, forget_type, bl_delay, True,
                       negative_window_size, validation_window, bl_regression,
                       label_days, bl_transfer, bl_ssd, file_format,
                       collector, random_seed, num_trees, down_ratio,
                       lamb_p, lamb_n, frac, bl_allocation)
        if not bl_transfer:
            fname = (sim.memory.cur_date -
                     datetime.timedelta(days=1)).isoformat()[0:10]
            sim.write_train_data(train_path, fname)
            #sim.write_test_data(test_path, fname)
            #sim.run()
        else:
            ## For transfer learning
            print(sim.memory.cur_date)
            fname = (sim.memory.cur_date -
                     datetime.timedelta(days=1)).isoformat()[0:10]
            sim.write_test_data(test_path, fname)

            for i in range(1, positive_window_size):
                sim.load()
                print(sim.memory.cur_date)
                fname = (sim.memory.cur_date -
                         datetime.timedelta(days=1)).isoformat()[0:10]
                sim.write_test_data(test_path, fname)
            sim.write_train_data(train_path, fname)
            #sim.run()

    if bl_load is False and bl_delay:
        for i in range(validation_window):
            sim.load()
            print(sim.memory.cur_date)
            fname = (sim.memory.cur_date -
                     datetime.timedelta(days=1)).isoformat()[0:10]

            #sim.write_test_data(test_path, fname)
            sim.write_train_data(train_path, fname)
            #sim.run()

    t_start_feature = sim.memory.basic_oper.sum_time_feature
    t_start_buffering = sim.memory.sum_time_buffering
    t_start_labeling = sim.memory.sum_time_labeling
    t_start_filtering = sim.memory.sum_time_filtering
    t_start_random_down = sim.allocate.sum_time_random_down
    t_start_allocation = sim.allocate.sum_time_allocation

    for ite in range(0, iter_days):
        print(sim.memory.cur_date)
        date = sim.memory.cur_date
        if bl_delay:
            sim.load()
            fname = (sim.memory.cur_date -
                     datetime.timedelta(days=1)).isoformat()[0:10]
            #sim.write_test_data(test_path, fname)
            sim.write_train_data(train_path, fname)
            #sim.run()
        else:
            sim.load()
            fname = (sim.memory.cur_date -
                     datetime.timedelta(days=1)).isoformat()[0:10]
            sim.write_test_data(test_path, fname)
            sim.write_train_data(train_path, fname)
            #sim.run()
    #print("iter days", iter_days)
    print("sum time feature",
          sim.memory.basic_oper.sum_time_feature - t_start_feature)
    print("sum time buffering",
          sim.memory.sum_time_buffering - t_start_buffering)
    print("sum time labeling", sim.memory.sum_time_labeling - t_start_labeling)
    print("sum time filtering",
          sim.memory.sum_time_filtering - t_start_filtering)
    print("sum time random", sim.allocate.sum_time_random_down - t_start_random_down)
    print("sum time allocation", sim.allocate.sum_time_allocation - t_start_allocation)

    #if bl_save:
    #    with open(path_save, 'wb') as f:
    #        pickle.dump(sim, f)


def usage(arg):
    print(arg, ":h [--help]")
    print("-s <start_date> [--start_date <start_date>]")
    print("-p <path_dataset> [--path <path_dataset>]")
    print("-l <path_load> [--path_load <path_load>]")
    print("-v <path_save> [--path_save <path_save>]")
    print("-c <path_features> [--path_features <path_features>]")
    print("-r <train_data_path> [--train_path <train_data_path>]")
    print("-e <test_data_path> [--test_path <test_data_path>]")
    print("-f <file_format> [--format <file_format>]")
    print("-o <option> [--option <option>]")
    print("-i <iter_days> [--iter_days <iter_days>]")
    print("-d <disk_model> [--disk_model <disk_model>]")
    print("-t <forget_type> [--forget_type <forget_type>]")
    print(
        "-w <positive_window_size> [--positive_window_size <positive_window_size>]"
    )
    print(
        "-L <negative_window_size> [--negative_window_size <negative_window_size>]"
    )
    print("-V <validation_window> [--validation_window <validation_window>]")
    print("-a <label_days> [--label_days <label_days>]")
    print("-F <date_format> [--date_format <date_format>]")
    print()
    print("Details:")
    print("path_load = load the Simulate class for continuing to process data")
    print(
        "path_save = save the Simulate class for continuing to process data next"
    )
    print(
        "file_format = file format of saving the processed data, arff by default"
    )
    print(
        "option = 1: enable regression (classification by default); 2: enable loading the Simulate class; 3: enable saving the Simulate class; 4: enable labeling; 5: enable transfer learning; 6: for Alibaba SSD datasets; 7: enable training data allocation"
    )
    print(
        "forget_type = \"no\" (keep all historical data) or \"sliding\" (sliding window), \"sliding\" by default"
    )
    print(
        "positive_window_size = size of the sliding time window, 30 days by default"
    )
    print(
        "negative_window_size = size of the window for negative samples in 1-phase downsampling, 7 days by default"
    )
    print(
        "validation_window = size of window for evaluation, 30 days by default"
    )
    print("label_days = number of extra labeled days")
    print("collector_id = collector name, e.g., collector1")
    print("lamb_p = the hyper-parameter lambda_p in customized Poisson sampling for positive samples")
    print("lamb_n = the hyper-parameter lambda_n in customized Poisson sampling for negative samples")
    print(
        "frac = the ratio of randomly downsampling negative samples. The value is between 0 and 1, where 1 means keeping all negative samples.")
    print("test = the goal that you test for. It should be \"performance\" or \"accuracy\"")


def get_parms():
    str_start_date = "2015-01-01"
    date_format = "%Y-%m-%d"
    path = "~/trace/smart/all/"
    train_path = "./train/"
    test_path = None
    path_load = None
    path_save = None
    bl_delay = False
    bl_load = False
    bl_save = False
    bl_regression = False
    bl_transfer = False
    bl_ssd = False
    bl_allocation = False
    option = {
        1: "bl_regression",
        2: "bl_load",
        3: "bl_save",
        4: "bl_delay",
        5: "bl_transfer",
        6: "bl_ssd",
        7: "bl_allocation"
    }

    file_format = "arff"
    iter_days = 5
    #manufacturer = None  #'ST'
    #model = 'ST4000DM000'
    model = []
    features = [
        'smart_1_normalized', 'smart_5_raw', 'smart_5_normalized',
        'smart_9_raw', 'smart_187_raw', 'smart_197_raw', 'smart_197_normalized'
    ]
    corr_attrs = []
    path_features = None
    label = ['failure']
    forget_type = "sliding"
    label_days = None
    positive_window_size = 30
    negative_window_size = 7
    validation_window = 30
    client_id = None
    random_seed = 0
    num_trees = config.NUM_TREES
    down_ratio = 1
    lamb_p = 6
    lamb_n = 1
    frac = None
    test = None

    try:
        (opt, args) = getopt.getopt(
            sys.argv[1:], "hs:p:l:v:c:r:e:f:o:i:d:t:w:L:V:a:F:C:R:D:P:N:x:T:", [
                "help", "start_date", "path", "path_load", "path_save",
                "path_features", "train_path", "test_path", "file_format",
                "option", "iter_days", "disk_model", "forget_type",
                "positive_window_size", "negative_window_size",
                "validation_window", "label_days", "date_format",
                "collector_id", "random_seed", "down_ratio", "lamb_p", "lamb_n",
                "frac", "test"
            ])
    except:
        usage(sys.argv[0])
        print("getopts exception")
        sys.exit(1)

    for o, a in opt:
        if o in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(0)
        elif o in ("-s", "--start_date"):
            str_start_date = a
        elif o in ("-p", "--path"):
            path = a
        elif o in ("-l", "--path_load"):
            path_load = a
        elif o in ("-v", "--path_save"):
            path_save = a
        elif o in ("-c", "--path_features"):
            path_features = a
        elif o in ("-f", "--file_format"):
            file_format = a
        elif o in ("-r", "--train_path"):
            train_path = a
        elif o in ("-e", "--test_path"):
            test_path = a
        elif o in ("-o", "--option"):
            ops = a.split(",")
            for op in ops:
                if int(op) == 1:
                    bl_regression = True
                elif int(op) == 2:
                    bl_load = True
                elif int(op) == 3:
                    bl_save = True
                elif int(op) == 4:
                    bl_delay = True
                elif int(op) == 5:
                    bl_transfer = True
                elif int(op) == 6:
                    bl_ssd = True
                elif int(op) == 7:
                    bl_allocation = True
        elif o in ("-i", "--iter_days"):
            iter_days = int(a)
        elif o in ("-d", "--disk_model"):
            model = a.split(",")
        elif o in ("-t", "--forget_type"):
            forget_type = a
        elif o in ("-w", "--positive_window_size"):
            positive_window_size = int(a)
        elif o in ("-L", "--negative_window_size"):
            negative_window_size = int(a)
        elif o in ("-V", "--validation_window"):
            validation_window = int(a)
        elif o in ("-a", "--label_days"):
            label_days = int(a)
        elif o in ("-F", "--date_format"):
            date_format = a
        elif o in ("-C", "--client_id"):
            client_id = a
        elif o in ("-R", "--random_seed"):
            random_seed = int(a)
        elif o in ("-D", "--down_ratio"):
            down_ratio = float(a)
        elif o in ("-P", "--lamb_p"):
            lamb_p = float(a)
        elif o in ("-N", "--lamb_n"):
            lamb_n = float(a)
        elif o in ("-x", "--frac"):
            frac = float(a)
        elif o in ("-T", "--test"):
            test = a

    if str_start_date.find("-") != -1:
        start_date = datetime.datetime.strptime(str_start_date, "%Y-%m-%d")
    else:
        start_date = datetime.datetime.strptime(str_start_date, "%Y%m%d")
    if path_features is not None:
        features = []
        with open(path_features, "r") as f:
            for line in f.readlines():
                features.append(line.strip())
        #print(features)

    if bl_ssd:
        columns = ['ds', 'model', 'disk_id'] + features
    else:
        columns = ['date', 'model', 'serial_number'] + label + features
    return (start_date, path, path_load, path_save, train_path, test_path,
            file_format, bl_delay, bl_load, bl_save, iter_days, model,
            features, label, columns, forget_type, positive_window_size,
            negative_window_size, validation_window, bl_regression, label_days,
            bl_transfer, bl_ssd, date_format, client_id, random_seed,
            num_trees, down_ratio, lamb_p, lamb_n, frac, bl_allocation, test)


if __name__ == "__main__":
    (start_date, path, path_load, path_save, train_path, test_path,
     file_format, bl_delay, bl_load, bl_save, iter_days, disk_model, features,
     label, columns, forget_type, positive_window_size, negative_window_size,
     validation_window, bl_regression, label_days, bl_transfer, bl_ssd, date_format,
     client_id, random_seed, num_trees, down_ratio, lamb_p, lamb_n, frac, bl_allocation, test) = get_parms()

    run_simulating(start_date, path, path_load, path_save, train_path,
                   test_path, file_format, iter_days, disk_model, features,
                   label, columns, forget_type, positive_window_size, bl_delay,
                   bl_load, bl_save, negative_window_size, validation_window,
                   bl_regression, label_days, bl_transfer, bl_ssd, date_format,
                   client_id, random_seed, num_trees, down_ratio, lamb_p, lamb_n, frac, bl_allocation, test)
