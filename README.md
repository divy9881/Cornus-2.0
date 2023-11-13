Sundial
=======

Sundial is a distributed OLTP database management system (DBMS). It supports a number of traditional/modern distributed concurrency control protocols, including WAIT_DIE, NO_WAIT, F1 (Google), MaaT, and TicToc. Sundial is implemented on top of [DBx1000](https://github.com/yxymit/DBx1000). 

The following two papers describe Sundial and DBx1000, respectively: 

[Sundial Paper](http://xiangyaoyu.net/pubs/sundial.pdf)  
Xiangyao Yu, Yu Xia, Andrew Pavlo, Daniel Sanchez, Larry Rudolph, Srinivas Devadas  
Sundial: Harmonizing Concurrency Control and Caching in a Distributed OLTP Database Management System  
VLDB 2018
    
[DBx1000 Paper](http://www.vldb.org/pvldb/vol8/p209-yu.pdf)  
Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker  
Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores  
VLDB 2014

Setup Compute Node
----------------------

### Step 0: Edit IP Address and Local Path

- Edit src/ifconfig.txt
  - before "=l", one line corresponds to one compute instance.
  - after "=l", one line corresponds to one storage instance
- Edit info.txt
  - first line: user name
  - second line: absolute path to the repository

### Step 1: Setup SSH Key Authorization Locally

- assume there's a public key at <root>/.ssh/id_ed25519.pub
  - generate ssh key [link](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
- make sure each node can access the other node through sudo and you have 
  already tried so that there will not have pop-up question 
  asking about whether to add the ip address
- setup locally: ```python3 install.py setkey_local```

### Step 2: Install dependency

- install locally: ```python3 install.py install_local```
- copy current setup to other compute nodes and install remotely ```python3
  install.py install_remote```
  - troubleshooting: on each node run "install_local"

### Step 3: Setup SSH Key Authorization Remotely

- setup key authorization for each node ```python3
  install.py setkey_remote```
  - troubleshooting: command not returned.
    - sometimes first ssh access will spin on requiring input for 
    "The authenticity of host 'node-0 (x.x.x.x)' can't be established.
    ECDSA key fingerprint is SHA256:xxx.
    Are you sure you want to continue connecting (yes/no)?"
    - if this is the case, on each node, try executing ```sudo ssh <other node 
      ip>``` to bypass the question. 

### Step 4: More Setups

To set up library path and proto on all compute nodes:
```python3 install.py config_local```
```python3 install.py config_remote```

Setup Storage Service
----------------------

Edit ifconfig.txt on master ***compute node***, after "=l", use the ip of the (master) storage node (if you use multiple replicas)

### Customized Redis Setup

Install redis
```
git clone https://github.com/redis/redis.git
cd redis
make
cd
mkdir redis_data/
```

Open ```redis.conf``` to setup of Redis:

on Master
- change bind to bind 0.0.0.0 -::1 OR comment out bind to use default
- set up ```protected-mode``` (e.g. password), you can use ```protected-mode 
  no``` but it is dangerous for public cloud without setting ```requirepass```
- set ```appendonly yes```
- set ```appendfsync always```
- set ```dir <your desired path for log>```, e.g. 
  ```dir /users/scarletg/redis_data```
- set ```requirepass sundial-dev```
    
on Replica
- set ```replicaof <ip of the master> <port>```

On Master, start the service
```
cd src/
./redis-server ../redis.conf
```



Configuration & Execution
--------------------------

### Manual

- Set up config in src/config.h
- For each compute node, do the follow:
```
./tools/compile.sh
./tools/run.sh -Gn<node_id>
```

### Batch Tests for Experiments

- Different ways of setting up configurations:
  1. directly edit src/config.h
  2. edit .json file and use command line arguments to overwrite some configs.

- If using option 2: 

  - template for starting all compute nodes:
  ```
  python3 run_exp.py CONFIG=exp_profiles/<name of config file>.json NODE_ID=<current node id> <optional args overwriting config.h>
  ```
  
  - example usage:
  ```
  python3 run_exp.py CONFIG=exp_profiles/ycsb_zipf.json NODE_ID=0 
  FAILURE_ENABLE=false MODE=debug
  ```
  - this script allows for list type for each config in .json, and it will 
  enumerate all the possible combinations of the config. 
  i.e. the number of tests in total will be the product of the length of every list. 
  - Notes on MODE:
    - if MODE=compile, it will just compile on all nodes without execution
    - if MODE=debug, it will compile and run, but not write output file
    - if MODE=release, it will write output file stats.json to outputs/ after execution

[comment]: <> (collect results from all nodes:)

[comment]: <> (go to tools/collect_result_remote.py and change the user to your cloudlab user name)

[comment]: <> (```)

[comment]: <> (cd tools/)

[comment]: <> (python3 collect_result_remote.py <exp_name> # exp_name will be name of your json file, in this case it's ycsb_zipf)

[comment]: <> (```)



Output 
------

The output file is stored in ```outputs/```. The output data should be mostly self-explanatory. More details can be found in system/stats.cpp.


Troubleshooting:
1. ```Package re2 was not found in the pkg-config search path```
Solution: Clone and build `re2`:
```
git clone https://github.com/google/re2.git
cd <re2 directory>
make
make install
make testinstall
```
2. Include error in #include <cpp_redis/cpp_redis>
In the src/Makefile, append the `INCLUDE` flag with the following path:  
`-I/<path_to_cpp_redis>/includes`

3. Include error in tcp_client.hpp
```
compilation terminated.
make: *** [Makefile:48: system/txn_client.o] Error 1
make: *** [Makefile:48: system/global.o] Error 1
In file included from /users/itsnasa/cpp_redis/includes/cpp_redis/cpp_redis:41,
                 from transport/redis_client.h:8,
                 from transport/rpc_server.cpp:6:
/users/itsnasa/cpp_redis/includes/cpp_redis/network/tcp_client.hpp:28:10: fatal error: tacopie/tacopie: No such file or directory
   28 | #include <tacopie/tacopie>
      |          ^~~~~~~~~~~~~~~~~
compilation terminated.
```
Solution:
```
git clone https://github.com/Cylix/tacopie  
cd <tacopie_directory>
make
```
Append the `INCLUDE` flag with the following path:
`-I/users/itsnasa/tacopie/includes/`
