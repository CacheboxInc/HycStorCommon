#!/usr/bin/python3

BLOCK_SIZES = (8192, 16*1024, 32*1024)
BATCH_SIZE = (1, )
IODEPTH = (32, 64)
OBJECTS = (2048*1024, )
WRITE_MIX = (0, 30, 50, 70, 100)
RUNTIME = 15*60
RANDOM = ('false', 'true')
BIN_SIZE = (1, 2, 4)
IPS = "10.10.20.36"

def RestartAeroSpike(aero_ip):
    print("ssh root@%s /root/cleanup.sh" % (IPS))
    print("sleep 30")
    pass

def RunBenchmark():
    for nobjects in OBJECTS:
        for bs in BLOCK_SIZES:
            for depth in IODEPTH:
                for nbatch in BATCH_SIZE:
                    for nbins in BIN_SIZE:
                        for writes in WRITE_MIX:
                            for random in RANDOM:
                                RestartAeroSpike(IPS);
                                cmd = "./AeroBench "
                                cmd += "-block_size=%d " % (bs)
                                cmd += "-batch_size=%d " % (nbatch)
                                cmd += "-objects=%d " % (nobjects)
                                cmd += "-write_mix_percent=%d " % (writes)
                                cmd += "-iodepth=%d " % (depth)
                                cmd += "-runtime=%d " % (RUNTIME)
                                cmd += "-random=%s " % (random)
                                cmd += "-ips=%s " % (IPS)
                                cmd += "-port=3000 "
                                cmd += "-bins=%d " % (nbins)
                                print(cmd)

RunBenchmark()
