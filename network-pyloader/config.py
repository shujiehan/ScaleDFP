TIMEOUT=1

TEST_PERF = "performance"
TEST_ACC = "accuracy"

NUM_COLLECTORS = 4
COORD_ADDR = '127.0.0.1:50051'
# whether to write the training data into the local file system of receivers
WRITE = True

NUM_TREES = 30
# TODO: config this map before running
RECEIVER_MODEL_MAP = {
    "receiver0": ["weight_%d" % i for i in range(0, NUM_TREES)],
    #"receiver0": ["weight_%d" % i for i in range(0, NUM_TREES//2)],
    #"receiver1": ["weight_%d" % i for i in range(NUM_TREES//2, NUM_TREES)],
}

RECEIVER_ADDR_MAP = {
    "receiver0": '127.0.0.1:5552',
}

COLLECTOR_ADDRESS_MAP = {
    "collector1": '127.0.0.1',
    "collector2": '127.0.0.1',
    "collector3": '127.0.0.1',
    "collector4": '127.0.0.1',
}
