# Dataflow Awesome Managing Engine

The easiest dataflow managing framework - **currently under construction.**

DAME solves/facilitates:
 - Building datasets from files / folders
 - Transforming data in the right order
 - Saving transformed data - once computed never compute it again
 - Choosing the best transformation from a few configurations

Great for working with numpy, pyTorch and more.

## Vision

**Technically**:
 - Compute stages:
   1. Sources - get data element
   2. Transforms - compute something out of available data
   3. Reducers - compute something on the whole dataset
 - Combining data sources
 - Compute only what you need - optimized performance via DAGs
 - Backup and cache, after stages, support for custom serializers
 - Ranking various configurations
 - (Optional) Parallel processing

**Priorities**:
 - Easy to use
 - Batteries included
 - Little overhead - take advantage of fastest tools available
 - Integrates seamlessly with other tools
 - Expandable

**Nice to have**:
 - Few python dependencies
 - Integrate tqdm
 - DAG output

## Backlog:

### 1.0.0:
  * [x] - Dataset - compute items via Sources and Transforms
  * [x] - Dataset - compute stage by stage, (assequence)
  * [x] - Dataset - validate Transforms 
  * [x] - Dataset - (_Stages) DAG computations
  * [ ] - **WIP** - Dataset - Automatic Transform versioning based on source and attrs
  * [ ] - **WIP** - Dataset - Enable Metadata Storage
  * [ ] - Dataset - Enable Storage & Caching
  * [ ] - Storage - Pickle
  * [ ] - Cache - Ring
  * [ ] - Dataset - Compute by chunks for efficient cache
  * [ ] - Dataset - Enable workers
  * [ ] - Workers - MultiThreading / MultiProcessing
  * [ ] - Reducer - Scoring
  * [ ] - Reducer - Ranking configurations
  * [ ] - Reducer - Find optimal parameters
  * [ ] - Transform - Mapping Transform, Sequential transform
  * [ ] - Dataset - Autodelete unrequired objects form memory (Autosequential)
  * [ ] - Docs - Dame tutorial & more tests
  * [ ] - TODOS - Solve left todos from the code


#### Storage/Cache options:

 * [ ] Pickle
 * [ ] Joblib
 * [ ] Redis
 * [ ] Sqlite
 * [ ] PyTables
 * [ ] Parquet/Dask


### 2.0.0 Ideas:
  * Easy reuse Dame transforms in Luigi/Dask/Apache Hadoop
  * More built-in storage and cache options
  * Built-in datasets like torchvision.MNIST etc
  * Module for managing on disk datasets. GUI? Conversion between:
    - Pytorch ImageFolder
    - Images + csv
    - Some Other

### Development:
  * [ ] - tox - build
  * [ ] - tox - publish
  * [ ] - hosting docs on readthedocs
  * [ ] - tox - publish docs
  * [ ] - coverage
  * [ ] - badges
  