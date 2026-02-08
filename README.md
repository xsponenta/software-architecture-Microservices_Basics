# software-architecture-Microservices_Basics

## Prerequisites

- Python 3.11+
- pip
- Docker and Docker Compose

## Installation

1. Clone the repository:
```bash
git clone https://github.com/xsponenta/software-architecture-Microservices_Basics
cd software-architecture-Microservices_Basics
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Generate gRPC protobuf files (if not already generated):
```bash
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=. \
    --grpc_python_out=. \
    ./proto/logging.proto
```

## Running Locally

```bash
./run.sh 
```
