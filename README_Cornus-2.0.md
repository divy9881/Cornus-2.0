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
# Follow Step 0, Step 1, Step 2, and Step 4
# Follow Redis setup from the README
# Setup config.h which is missing in the codebase
# Com
$ ./tools/compile.sh
```