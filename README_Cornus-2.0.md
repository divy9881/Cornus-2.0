### Setup Docker Container
```
$ docker run -it -v $pwd/Cornus-2.0:/Cornus-2.0 ubuntu bash
$ apt-get update
$ apt-get install g++ ssh -y
```

### Generate Public-Private key-pair for remote SSH
```
# Create SSH key using ED25519 Cryptographyic Algorithm and CloudLab username
$ ssh-keygen -t ed25519 -C "<identifier>"
```

### Setup public key in the CloudLab
```
# Display the public key which was generated
$ cat /root/.ssh/id_ed25519.pub
```

- Copy the public key and paste it to the text-field of manage ssh keys in the CloudLab
- Create the experiment instance from existing experiment in the project on Sundial DB

### SSH to remote node on the CloudLab
```
$ ssh -i /root/.ssh/id_ed25519 dspatel6:<cloudlab-node-name>
# If above command fails, try removing known_hosts file
$ rm -rf /root/.ssh/known_hosts
```

### Setup Sundial DB
```
$ git clone https://github.com/divy9881/Cornus-2.0/
$ cd Cornus-2.0
$ git checkout divy/setup-sundial

$ PROTOC_ZIP=protoc-3.15.8-linux-x86_64.zip
$ curl -OL https://github.com/google/protobuf/releases/download/v3.15.8/$PROTOC_ZIP
$ sudo unzip -o $PROTOC_ZIP -d /usr/local bin/protoc
$ sudo unzip -o $PROTOC_ZIP -d /usr/local include/*
$ rm -f $PROTOC_ZIP

# Follow Step 0, Step 1, Step 2

# Run following commands one-by-one:
$ cd $HOME
$ cd grpc/test/distrib/cpp/
$ cp ${HOME}/Cornus-2.0/tools/run_distrib_test_cmake.sh ./

# The following command should take long time ideally
$ ./run_distrib_test_cmake.sh
$ export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
$ export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# Make sure whether gRPC is installed correctly by testing, following command, it should output a path(not null)
$ which grpc_cpp_plugin

# Follow Step 4

# Follow Redis setup from the README

# Setup config.h which is missing in the codebase

# Compile the Cornus source files
$ ./tools/compile.sh
```