# Hdfs2Qiniu

## 简介

该工具主要用来将HDFS（Hadoop 文件系统）中的数据同步到七牛公有云端存储或者七牛私有云存储。该工具使用简单方便，支持大规模文件的并发同步以及其他各种功能，满足不同的同步需求。

## 原理

HDFS -> Hdfs2Qiniu读取文件内容 -> 上传内容到七牛存储 

## 功能

1.  支持HDFS文件系统中文件批量上传到七牛存储
2.  支持HDFS文件系统中文件并发上传到七牛存储
3.  支持HDFS文件上传到七牛公有云和私有云存储
4.  支持指定HDFS的配置文件路径支持多HDFS系统
5.  支持文件的存储类型设置，可以是普通存储或者低频存储
6.  支持指定文件保存在七牛存储中的统一前缀
7.  支持忽略HDFS文件系统中文件相对路径来保存上传后文件
8.  支持覆盖云端存储中已有的同名文件，默认不覆盖
9.  支持检测云端存储中是否已有同名，同内容文件，可以选择跳过不上传
10.  支持检测HDFS文件系统中新增文件并上传
11. 支持本地保存已上传文件的记录，下次文件无改动则不上传
12. 支持跳过HDFS文件系统中大小为0的文件不上传
13. 支持跳过HDFS文件系统中文件名包含指定前缀的文件不上传
14. 支持跳过HDFS文件系统中文件路径包含指定前缀的文件不上传
15. 支持跳过HDFS文件系统中文件名以指定后缀的文件不上传
16. 支持跳过HDFS文件系统中文件名包含特定字符串的文件不上传
17. 支持输出日志到指定文件，如果不指定，默认会在HOME目录下写日志
18. 支持输出日志到标准输出，用来诊断上传问题
19. 支持设置日志文件中输出的日志级别
20. 支持日志的按天Rotate功能

## 下载

可以下载打包好的可执行JAR包：[hdfs2qiniu-v1.5.jar](http://devtools.qiniu.com/hdfs2qiniu-v1.5.jar)

参考配置文件：[upload.properties](https://github.com/jemygraw/Hdfs2Qiniu/blob/master/src/main/resources/upload.properties)


## 使用

该工具的基本使用方式为：

```
$ java -jar hdfs2qiniu.jar uploadConfigFile worker
```

|参数|描述|
|-------|------|
|uploadConfigFile|文件上传的配置文件路径|
|worker|文件并发上传的线程数量，可选，默认为1|


下面我们根据支持的功能讲解`uploadConfigFile`的内容。该配置文件的格式为`properties`的格式，就是简单的`key=value`的组织方式。

该配置文件支持的参数列表如下：

|参数|描述|可选|
|------|------|------|
|hdfs_configs|该文件为HDFS的配置文件路径，比如`core-site.xml`和`hdfs-site.xml`的文件路径，如果有多个文件路径，每个路径之间用逗号(`,`分隔)|必填|
|access_key|账号的`access key`|必填|
|secret_key|账号的`secret key`|必填|
|src_dir|待上传的HDFS文件系统目录，必须以`hdfs://`开头|必填|
|bucket|文件存储目标空间名称|必填|
|up_host|这个参数用来设置文件上传的入口域名，可以用于私有云的情况下自定义上传域名，上传公有云时，默认不设置|可选|
|rs_host|这个参数用来设置检查文件是否存在的域名，可以用于私有云的情况下自定义域名，上传公有云时，默认不设置|可选|
|key_prefix|上传后文件名保存时的附加前缀，比如批量上传的时候可以指定一个文件前缀用以区分不同的任务等，默认为空|可选|
|ignore_dir|设置文件上传后只以HDFS文件路径中的最后一部分文件名作为保存的文件名，如果设置了`key_prefix`，这个名字还会带上这个前缀|可选|
|check_exists|检查空间中是否已存在同名的文件，如果存在则默认情况下会比对本地文件和空间文件的文件大小，如果大小发生变化，则认为文件已变化|可选|
|check_hash|在设置`check_exists`的情况下，如果空间已存在同名文件，那么`check_hash`指定是否已更加严格的hash比对来判断文件是否已发生变化|可选|
|overwrite|该参数指定了在上述检测文件是否变化的情况下，如果发现文件已发生变化后的动作，如果设置为true，则覆盖上传，否则默认不采取任何动作|可选|
|rescan_local|该参数指定为true的时候会重新遍历HDFS文件系统来发现新增的文件，默认情况下为false|可选|
|skip_empty_file|默认为true表示跳过大小为0的文件不上传，如果需要这些文件，可以自行指定为false|可选|
|skip_file_prefixes|指定一个或者多个文件的名称（文件路径最后一部分）前缀，彼此用逗号分隔，上传的时候如果发现文件名以这些前缀中的任意一个开头，则跳过不上传|可选|
|skip_path_prefixes|指定一个或者多个文件的路径前缀，彼此用逗号分隔，上传的时候如果发现文件相对路径以这些前缀中的任意一个开头，则跳过不上传|可选|
|skip_suffixes|指定一个或者多个文件路径的后缀，彼此用逗号分隔，上传的时候如果发现文件相对路径以这些后缀中的任意一个结尾，则跳过不上传|可选|
|log_file|指定上传日志的输出文件，默认不指定的情况下，会在`$HOME/.hdfs2qiniu`下面创建上传任务文件夹，并把日志保存在里面|可选|
|log_level|指定上传日志的输出级别，支持`debug`，`info`，`warn`，`error`几个选项。|可选|
|log_rotate|指定日志文件是否按天切换，指定为true的情况下，上传日志文件会按照天分割内容|可选|
|log_stdout|指定日志文件是否输出一份到标准终端，一般调试的时候可以设置|可选|

## 案例

其中`$HADOOP_HOME`需要替换为实际的路径，配置文件不支持读取环境变量，这里方便演示，所以这样写。

### 最简单的上传配置

```
hdfs_configs=$HADOOP_HOME/etc/hadoop/core-site.xml
access_key=xxx
secret_key=xxx
src_dir=hdfs://localhost:9000/user/jemy/testdir
bucket=xxx
```

这个配置是最小化的上传配置，符合大部分场景下的上传需求。

### 可以额外指定一个前缀

```
key_prefix=images/
```

这样对于本地文件`hdfs://localhost:9000/user/jemy/testdir/1.png`这个文件，在指定`src_dir`为`hdfs://localhost:9000/user/jemy/testdir`的情况下，目标空间中最终保存的文件名为`images/1.png`。

### 忽略本地文件的相对路径

```
ignore_dir=true
```

这样对于本地文件`hdfs://localhost:9000/user/jemy/testdir/2017/07/1.png`这个文件，在指定了`src_dir`为`hdfs://localhost:9000/user/jemy/testdir`的情况下，目标空间中最终保存的文件名为`1.png`，如果在指定`ignore_dir`为`true`的同时，还指定了`key_prefix`，那么这个前缀会附加在最终的文件名之前，变成`images/1.png`。

### 私有化部署的存储

如果是私有部署的存储系统，那么上传的入口和资源管理域名和公有云不一样，这个时候可以指定下面的两个参数来支持上传到私有存储：

```
up_host=http://up.qos.customer.com
rs_host=http://rs.qos.customer.com
```

### 增量上传

默认的情况下，在一次性遍历完HDFS目录之后，每次运行相同命令上传的时候，都不会去检查HDFS中新增的文件。如果上一次遍历完成后，只上传了一半的文件就中止了，那么再次运行上传命令，会跳过已经上传成功的文件不上传。这些已经上传成功的文件是记录在本地的数据库里面，所以很容易跳过。

当有些情况下，希望每次上传都能再去遍历一次HDFS系统，以发现新的文件进行上传，这个时候可以指定`rescan_local`为`true`。

```
rescan_local=true
```

### 检查空间已有文件

有一些使用场景：
（1）在有些情况下，删除了空间中一些已上传的文件，这个时候由于这些已上传的文件记录已经记录在本地数据库中，再次上传的时候会自动跳过。
（2）另一个情况是可能从多台服务器上传文件，有些文件已经从A服务器传到了空间，而B服务器上面也有这个文件，这个时候理论上不再需要上传这个文件了，所以需要提前了解空间中是否已经存在相同文件。

为了解决上面的问题，我们引入了`check_exists`选项，默认情况下为`false`。当指定为`true`的时候，会在每个文件上传之前，根据文件名去空间查询一下，是否已存在同名的文件，如果不存在，则进行上传。

当然如果发现同名文件已存在，这个时候默认情况下会去检查文件的大小是否一致来判断是否需要上传新文件，有时候为了让这种判断更加严格，可以通过比对文件的hash来判断文件内容是否一致。这个时候，可以指定`check_hash`为`true`。

不过由于检查文件的hash需要本地把文件的内容hash计算一次，对于大的文件可能比较影响性能，注意根据实际需要选择是否开启这个选项。

```
check_exists=true
check_hash=false
```

如果开启了`check_exists`选项，那么本地数据库记录的该文件上传信息会被忽略。


### 文件跳过规则

我们提供了几种根据文件路径或者文件名（文件路径的最后一部分）来跳过相关文件不进行上传的功能。

（1）skip_empty_file

该选项默认为`true`，会跳过HDFS中文件大小为0的文件不上传。因为实践中，HDFS中会存在一些比较多的空文件，实际上并没有用处，我们可以不上传。如果你需要这些空文件，可以设置这个选项为`false`。

（2）skip_file_prefixes

该选项支持设置一些文件名称前缀列表，比如你需要跳过一些文件，这些文件名以`test`和`tmp`字符串为前缀，那么上传的时候可以跳过这些文件不上传。

```
skip_file_prefixes=test,tmp
```

那么，对于文件`hdfs://localhost:9000/user/jemy/testdir/images/test_1.png`，这个文件在上传的时候，当指定`src_dir`为`hdfs://localhost:9000/user/jemy/testdir/`时，我们发现文件相对路径是`images/test_1.png`，文件名是`test_1.png`，那么这个文件会跳过不上传。

（3）skip_path_prefixes

```
skip_path_prefixes=test/,tmp/
```

这个选项和`skip_file_prefixes`的区别在于，它判断相对路径的前缀是否在需要跳过的文件列表，比如上面的例子，文件的相对路径是`images/test_1.png`，这个文件路径不是以`test/`或者`tmp/`开头，那么就不跳过，需要上传。

（4）skip_suffixes

这个选项比较简单，文件相对路径以指定的后缀结尾的，则跳过不上传。

### 日志规则

（1）log_file

指定上传日志的输出文件，默认不指定的情况下，会在`$HOME/.hdfs2qiniu`下面创建上传任务文件夹，并把日志保存在里面

（2）log_level

指定上传日志的输出级别，支持`debug`，`info`，`warn`，`error`几个选项。


（3）log_rotate

指定日志文件是否按天切换，指定为true的情况下，上传日志文件会按照天分割内容。

（4）log_stdout


指定日志文件是否输出一份到标准终端，一般调试的时候可以设置。

参考设置：

```
log_file=logs/upload.log
log_level=info
log_rotate=true
log_stdout=false
```

