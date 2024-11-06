<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Building Beam Python SDK Image Guide

You can build a docker image if your local environment has Java, Python, Golang
and Docker properly. Try
`./gradlew :sdks:python:container:py<PYTHON_VERSION>:docker`. For example,
`:sdks:python:container:py310:docker` makes `apache/beam_python3.10_sdk`
if it is successful. You can follow this guide to try the image building from a
VM if the build fails in your local environment.

## Create VM

The steps in this guide are for Debian 11. You can create a GCE instance in
Google Cloud.

```shell
gcloud compute instances create beam-builder \
  --zone=us-central1-a  \
  --image-project=debian-cloud \
  --image-family=debian-11 \
  --machine-type=n1-standard-8 \ 
  --boot-disk-size=20GB \
  --scopes=cloud-platform
```

Login to the VM.

```shell
gcloud compute ssh beam-builder --zone=us-central1-a --tunnel-through-iap
```

Update the apt package list.

```shell
sudo apt-get update
```

> NOTE: The `cloud-platform` is recommended to avoid permission issues with
> Google Cloud Artifact Registry. The image build needs a large disk size. You
> can see "no space left on device" error at build with the
> default size 10GB.
>
See [the gcloud doc](https://cloud.google.com/sdk/gcloud/reference/compute/instances/create#--boot-disk-size)
> for details.

## Install Java

You need Java to run Gradle build.

```shell
sudo apt-get install -y openjdk-11-jdk
```

## Install Golang

```shell
curl -OL  https://go.dev/dl/go1.23.2.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.2.linux-amd64.tar.gz
```

```shell
export PATH=:/usr/local/go/bin:$PATH
```

Confirm the Golang version

```shell
go version

# Expected output:
# go version go1.23.2 linux/amd64
````

> [!NOTE]
> Old Go version (e.g. 1.16) will fail at `:sdks:python:container:goBuild`.

## Install Python

This guide uses Pyenv to manage multiple Python versions.
Reference: https://realpython.com/intro-to-pyenv/#build-dependencies

```shell
# Install dependencies 
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev 

# Install Pyenv
curl https://pyenv.run | bash

# Add pyenv to PATH.  
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

Install Python 3.9 and set the Python version.

```shell
pyenv install 3.9
pyenv global 3.9
```

Confirm the python version.

```shell
python --version

# Expected output:
# Python 3.9.17
``` 

> [!NOTE]
> You can use a different Python version for building with [
`-PpythonVersion` option](https://github.com/apache/beam/blob/v2.60.0/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy#L2956-L2961)
> to Gradle task run. Otherwise, you should have `python3.9` in the build
> environment for Apache Beam 2.60.0 or later (python3.8 for older Apache Beam
> versions).

## Install Docker

```shell
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
```

```shell
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
```

```shell
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

You will need to run `docker` command without the root privilege. You can do
this by adding your account to the docker group.

```shell
sudo usermod -aG docker $USER
newgrp docker
```

Confirm if you can run a container without the root privilege.

```shell
docker run hello-world
```

## Install Git

Git is not necessary for building Python SDK image. Git is just used to download
the Apache Beam code in this guide.

```shell
sudo apt-get install -y git
```

## Build Beam Python SDK Image

Download Apache Beam
from [the Github repository](https://github.com/apache/beam).

```shell
git clone https://github.com/apache/beam beam
cd beam
```

Modify the code. For
example, [the Python SDK worker boot application](https://github.com/apache/beam/blob/v2.60.0/sdks/python/container/boot.go).

```shell
./gradlew :sdks:python:container:py310:docker
```

If the build is successful, you can see the built image with the command.

```shell
docker images
```

```text
REPOSITORY                   TAG       IMAGE ID       CREATED              SIZE
apache/beam_python3.10_sdk   2.60.0    33db45f57f25   About a minute ago   2.79GB
```

## Push to Repository

### Google Cloud Artifact Registry

```shell
docker tag apache/beam_python3.10_sdk:2.60.0 us-central1-docker.pkg.dev/<my-project>/<my-repository>/apache/beam_python3.10_sdk:2.60.0-custom
docker push us-central1-docker.pkg.dev/<my-project>/<my-repository>/apache/beam_python3.10_sdk:2.60.0-custom
```

### Docker Hub

```shell
docker tag apache/beam_python3.10_sdk:2.60.0 <my-account>/beam_python3.10_sdk:2.60.0-custom
docker push <my-account>/beam_python3.10_sdk:2.60.0-custom
```

