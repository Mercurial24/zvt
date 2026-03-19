# 中国银河证券星耀数智 AmazingData 开发手册

中国银河证券星耀数智 · AmazingData 开发手册

---

1. 版本说明

文档管理信息表
1.1

| 主题 | 中国银河证券星耀数智AmazingData 开发手册 |
| --- | --- |
| 文档版本 | V1.0.24 |
| Python SDK 版本 | V1.0.24 |
| 创建时间 | 2025 年7月10日 |
| 最新发布日期 | 2025 年12月16日 |

功能介绍
2.

本文档是tgw的SDK开发指南，包含了对API接口的说明以及示例，用于指引开发人
员通过tgw金融数据功能接口进行数据接收和查询的开发，如需参考或使用本项目，需要提

前联系官方获取权限。

金融数据服务
2.1

金融数据功能，是指用户使用 C++、Python 以及其他本功能可支持的程序设计语言或
用户端页面，获取公司通过对证券交易所等渠道的公开信息加工而成的行情数据、金融资讯

数据等金融数据的功能。

## 2.2 数据详情

行情数据
1)

| 品种 | 数据类型 | 数据起点 | 说明 | 是否支持 实时订阅 |
| --- | --- | --- | --- | --- |
| 股票 | Level-1快照 、K 线数据 | 2013年至今 | 上交所、深交所、北交所 | 是 |
| 指数 | Level-1快照 、K 线数据 | 上交所、深交 所、北交所 | 是 |  |
| 债券 | Level-1快照 、K 线数据 | 上交所、深交所 | 是 |  |
| 场内基金 | Level-1快照 、K 线数据 | 上交所、深交所 | 是 |  |
| 期权 | Level-1快照 、K 线数据 | 2015年至今 | 深交所 ETF 期权、上交所ETF期权 | 是 |
| 港股通 | 港股通行情快照 | 2023年至今 | 上交所、深交所 | 是 |
| 期货 | Level-1快照 、K 线数据 | 2010年4月至 | 中金所今 | 是 |

- 2013年至今

| 2013年6月至 今 | 大商所 | 是 |
| --- | --- | --- |
| 2011年1月至 | 郑商所今 | 是 |
| 2019年8月至 今 | 上期所 | 是 |
| 2019年8月至 | 上海国际能源今 | 是交易中心所 |

2) 基础数据

- 每日最新证券信息，交易日早上9点前更新
- 复权因子
- 每日最新代码表，交易日早上9点前更新
- 历史代码表
- 交易日历

财务数据
3)

- 资产负债表
- 现金流量表
- 利润表
- 业绩快报
- 业绩预告

4) 股东股本数据

- 十大股东数据
- 股东户数
- 股本结构
- 股权冻结/质押
- 限售股解禁

股东权益数据
5)

- 分红数据
- 配股数据

6) 融资融券数据

- 融资融券成交汇总
- 融资融券交易明细

7) 交易异动数据

- 龙虎榜
- 大宗交易

开发指南
3. python

版本与下载
3.1 SDK

### 3.1.1 wheel 文件版本

| wheel文件名 | 操作系统 | Python版本 |
| --- | --- | --- |
| tgw-1.*.*-py3-none-any.whl | Linux/ Windows | Python 3.8 Python 3.9 Python 3.10 Python 3.11 Python 3.12 Python 3.13 |
| AmazingData-1.*.*-cp38-none-any.whl | Linux/ Windows | Python 3.8 Python 3.9 Python 3.10 Python 3.11 Python 3.12 Python 3.13 |

### 3.1.2 wheel 文件下载路径

1. 银河网盘

https://cloud.chinastock.com.cn/p/DSG36jYQx2IY_Y8CIAA
公众号“中国银河证券星耀数智”
2.
路径：“业务介绍”——“安装包下载”

## 3.2 SDK 运行环境

推荐运行环境配置
3.2.1 Linux

| 类型 | 最低配置 | 推荐配置 |
| --- | --- | --- |
| 处理器 | 2.10GHz,4核 | 2.10GHz,8核 |
| 内存 | DDR4 4GB | DDR4 4GB |
| 硬盘 | 200G 机械硬盘/SSD | 480G 机械硬盘/SSD |
| 网卡 | 普通网卡 | 普通万兆网卡 |
| 操作系统 | REDHAT 7.2/7.4/7.6 | REDHAT 7.2/7.4/7.6 |

推荐运行环境配置
3.2.2 Windows

| 类型 | 最低配置 | 推荐配置 |
| --- | --- | --- |
| 处理器 | 2.60GHz，4核 | 2.60GHz，8核 |
| 内存 | DDR4 4GB | DDR4 4GB |
| 硬盘 | 200G 机械硬盘/SSD | 480G 机械硬盘/SSD |
| 网卡 | 普通网卡 | 普通万兆网卡 |
| 操作系统 | Windows 10(64位) | Windows 10(64位) |

安装
3.3 SDK

### 3.3.1 tgw 安装

pip install tgw-1.7.1-py3-none-any.whl

安装
3.3.2 AmazingData

选择对应的python版本

pip install AmazingData-1.0.0-cp312-none-any.whl

## 3.4 Python 开发步骤

登录AmazingData之后，实现数据获取。

登录
3.4.1 AmazingData

（1）所有数据接口调用前，必须登录

（2）import AmazingData库，填写账号、密码、ip/port等信息，调用登录api。

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

### 3.4.2 调用数据接口

查询接口调用
3.4.2.1

（1）登录api；

（2）实例化对应的数据查询类；

（3）调用查询数据接口，获取数据；

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

- # 第二步 实例化对应的数据查询类

```python
base_data_object = ad.BaseData()
```

- # 第三步，调用查询数据接口，获取数据

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
```

订阅接口调用
3.4.2.2

（1）登录api；

（2）实例化对应的数据查询类；
（3）实例化数据订阅类；

（4）用装饰器装饰回调函数，接收订阅数据；

（5）订阅数据执行；

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

- # 第二步 输入标的代码列表

```python
base_data_object = ad.BaseData()
```

```python
etf_code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
```

- # 第三步 实例化数据订阅类

```python
sub_data = ad.SubscribeData()
```

- # 第四步 用装饰器装饰回调函数，接收订阅数据

```python
@sub_data.register(code_list=etf_code_list, period=ad.constant.Period.snapshot.value)
```

```python
def onSnapshot(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
```

- print(period, data)
- # 第五步 订阅数据执行

```python
sub_data.run()
```

接口详细
3.5 API

基础接口
3.5.1

登录
3.5.1.1

调用任何数据接口之前，必须先调用登录接口。

SDK的账号、密码、ip和端口号需联系您的开户营业部申请开通权限之后获取。

函数接口：login

功能描述：api 登陆

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| username | str | 是 | 账号 |
| password | str | 是 | 密码 |
| ip | str | 是 | 服务器ip |
| host | int | 是 | 服务器端口号 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

#### 3.5.1.2 登出

函数接口：logout
功能描述：api 退出登录链接 ，必须在登录状态下，才可使用；正常使用情况

下，无需使用此接口

| 名称 | 类型 | 说明 |
| --- | --- | --- |
| username | str | 用户名 |

更新密码
3.5.1.3

函数接口：update_password

功能描述：更新密码接口，必须先登录才能修改密码

| 名称 | 类型 | 说明 |
| --- | --- | --- |
| username | str | 用户名 |
| old_password | str | 旧密码 |
| new_password | str | 新密码 |

基础数据
3.5.2

#### 3.5.2.1 每日最新证券信息

- 函数接口：get_code_info
- 功能描述：获取每日最新证券信息，交易日早上9点前更新当日最新
- 输入：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| security_type | str | 否 | 代码类型security_type（见附录）， |

- 默认为 EXTRA_STOCK_A（上交 所A股、深交所A股和北交所的股 票列表）

输出：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| code_info | dataframe | index为股票代码 column为 symbol (证券简称) security_status（产品状态标志） pre_close (昨收价) high_limited (涨停价) low_limited ( 跌停价) price_tick (最小价格变动单位) |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_info = base_data_object.get_code_info(security_type='EXTRA_ETF')
```

每日最新代码表（沪深北）
3.5.2.2

- 交易日早上9点前更新

```python
函数接口：get_code_list
```

- 功能描述：获取代码表（每日最新），此接口无法获取历史代码表
- 输入：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| security_type | str | 否 | 代码类型security_type（见附录）， 默认为 EXTRA_STOCK_A（上交 所A股、深交所A股和北交所的股 票列表） |

输出参数：

| 返回值 | 数据类型 | 解释 |
| --- | --- | --- |
| code_list | list | 证券代码 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A')
```

每日最新代码表（期货交易所）
3.5.2.3

- 交易日早上9点前更新
- 函数接口：get_future_code_list
- 功能描述：获取代码表（每日最新），此接口无法获取历史代码表
- 输入：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| security_type | str | 是 | 代码类型 security_type(期货交易 所)（见附录），默认为EXTRA_F UTURE（期货, 包含中金所/上期所 /大商所/郑商所/上海国际能源交易 中心所） |

输出参数：

| 返回值 | 数据类型 | 解释 |
| --- | --- | --- |
| code_list | list | 证券代码 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_future_code_list(security_type='EXTRA_FUTURE')
```

每日最新代码表（期权）
3.5.2.4

- 交易日早上9点前更新
- 函数接口：get_option_code_list
- 功能描述：获取代码表（每日最新），此接口无法获取历史代码表
- 输入：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| security_type | str | 是 | 代码类型security_type期权)（见附 录），默认为EXTRA_ETF_OP（E TF期权, 包含上交所和深交所） |

输出参数：

| 返回值 | 数据类型 | 解释 |
| --- | --- | --- |
| code_list | list | 证券代码 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
```

复权因子（后复权因子）
3.5.2.5

函数接口：BaseData.get_backward_factor

功能描述：获取复权因子数据并本地存储，复权因子为根据交易所行情数据计算得出的后复

权因子；

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | lis[str] | 是 | 代码列表，支持股票、ETF |
| local_path | str | 是 | 本地存储复权因子数据的文件夹地址 |
| is_local | Bool | 是 | 是否使用本地存储的数据，默认为True |

- 注：
- （1）local_path
- 类似'D://AmazingData_local_data//'，只写文件夹的绝对路径即可
- （2）is_local
- True:
- 本地local_path有数据的情况下，从本地取数据，但无法从服务端获取最新的数据
- 本地local_path无数据的情况下，从互联网取数据，并更新本地local_path的数据
- False:从互联网取数据，并更新本地local_path的数据
- 输出：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| backward_factor | dataframe | index为交易日期 column为股票代码 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A')
```

```python
backward_factor = base_data_object.get_backward_factor(code_list, local_path='D://AmazingData_local_data//',
```

- is_local=False)

#### 3.5.2.6 复权因子（单次复权因子）

函数接口：BaseData.get_adj_factor
功能描述：获取复权因子数据并本地存储，复权因子为根据交易所行情数据计算得出的单次

复权因子；

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | lis[str] | 是 | 代码列表，支持股票、ETF |
| local_path | str | 是 | 本地存储复权因子数据的文件夹地址 |

| is_local | Bool | 是 | 是否使用本地存储的数据，默认为True |

- 注：
- （1）local_path
- 类似'D://AmazingData_local_data//'，只写文件夹的绝对路径即可
- （2）is_local
- True:
- 本地local_path有数据的情况下，从本地取数据，但有可能无法获取最新的数据
- 本地local_path无数据的情况下，从互联网取数据，并更新本地local_path的数据
- False:从互联网取数据，并更新本地local_path的数据
- 输出：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| adj_factor | dataframe | index为交易日期 column为股票代码 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A')
```

```python
adj_factor = base_data_object.get_adj_factor(code_list, local_path='D://AmazingData_local_data//',
```

- is_local=False)

历史代码表
3.5.2.7

- 函数接口：BaseData的get_hist_code_list
- 功能描述：获取历史代码表，先检查本地数据，再从服务端补充，最后返回数据输入参数：

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| security_type | security_type | str | str |
| start_date | int | 是 | 开始时间，闭区间 |
| end_date | int | 是 | 结束时间，闭区间 |
| local_path | local_path | str | str |

输出参数：

| 返回值 | 数据类型 | 解释 |
| --- | --- | --- |
| code_list | List[str] | 证券代码 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ',start_date=20240101,
```

- end_date=20240701, local_path=local_path)

#### 3.5.2.8 交易日历

函数接口：get_calendar

功能描述：获取交易所的交易日历

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| data_type | str | 否 | 选择返回数据的类型，默认为str ，可选datetime 或 str |
| market | str | 否 | 选择市场market（见附录），默认为SH（上海） |

输出参数：

| 返回值 | 数据类型 | 解释 |
| --- | --- | --- |
| calendar | List[int] | 日期 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

证券基础信息
3.5.2.9

函数接口：get_stock_basic

功能描述：获取指定股票列表的上市公司的证券基础数据，包含沪深北三个交易所，所有股

票（包含已退市标的）的中英文名称、上市日期、退市日期、上市板块等信息
输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深北三个交易所的代码列表，可见 示例 |

输出参数：

| 返回值 | 数据类型 | 解释 |
| --- | --- | --- |
| stock_basic | dataframe | column为stock_basic的字段 index为序号（无意义） |

stock_basic的字段说明：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| MARKET_CODE | string | 证券代码 |  |
| SECURITY_NAME | string | 证券简称 |  |
| COMP_NAME | string | 证券中文名称 |  |
| PINYIN | string | 中文拼音简称 |  |
| COMP_NAME_ENG | string | 证券英文名称 |  |
| LISTDATE | int | 上市日期 |  |
| DELISTDATE | int | 退市日期 |  |
| LISTPLATE_NAME | string | 上市板块名称 |  |
| COMP_SNAME_ENG | string | 英文名称缩写 |  |
| IS_LISTED | int | 上市状态 | 1：上市交易 3：终止上市 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A_SH_SZ')
```

```python
iinfo_data_object = ad.InfoData()
```

```python
stock_basic = iinfo_data_object.get_stock_basic (code_list)
```

#### 3.5.2.10 历史证券信息

函数接口：get_history_stock_status
功能描述：获取指定股票列表的上市公司的历史证券数据，以日度为频率，包含历史的涨跌

停、st、除权除息等信息

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类 似“D://AmazingData_local_data//” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 交易日，本地数据缓存方案 |
| end_date | int | 否 | 交易日，本地数据缓存方案 |

输出参数：

| 返回值 | 数据类型 | 解释 |
| --- | --- | --- |
| history_stock_status | dataframe | column为history_stock_status的字段 index为序号（无意义） |

history_stock_status的字段说明：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| MARKET_CODE | string | 证券代码 |  |

| TRADE_DATE | string | 日期 |
| --- | --- | --- |
| PRECLOSE | float | 前收价 |
| HIGH_LIMITED | float | 涨停价 |
| LOW_LIMITED | float | 跌停价 |
| PRICE_HIGH_LMT_RATE | float | 涨停价上限 |
| PRICE_LOW_LMT_RATE | float | 跌停价下限 |
| IS_ST_SEC | string | 是否ST |
| IS_SUSP_SEC | string | 是否停牌 |
| IS_WD_SEC | string | 是否除息 |
| IS_XR_SEC | string | 是否除权 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- today = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
history_stock_status = iinfo_data_object.get_history_stock_status(all_code_list)
```

北交所新旧代码对照表
3.5.2.11

- 函数接口：get_bj_code_mapping
- 功能描述：获取北交所的存量上市公司股票新旧代码对照表
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，首选从本地读取，读取失败 再从服务器取数据 False，以本地数据为基础，增量从服务器 取数据 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| bj_code_map ping | dataframe | column为bj_code_mapping的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
bj_code_mapping = iinfo_data_object.get_bj_code_mapping()
```

bj_code_mapping的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| OLD_CODE | string | 旧代码 |
| NEW_CODE | string | 新代码 |
| SECURITY_NAME | string | 证券简称 |
| LISTING_DATE | int | 上市日期 |

实时行情数据
3.5.3

- 实时行情订阅接口使用步骤
- （1） 实例化AmazingData的SubscribeData
- （2） 回调函数的装饰器传入code_list(代码表)和period(数据周期)两个参数
- （3） 回调函数中获取数据

指数实时快照
3.5.3.1

函数接口：onSnapshotindex

功能描述：交易所指数快照数据的实时订阅回调函数

输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交所 的指数 |
| period | Period | 是 | Period.snapshot.value |

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | 指数为SnapshotIndex（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type=' EXTRA_INDEX_A')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

```python
@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
```

```python
def onSnapshotindex(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
```

- print(period, data)

```python
sub_data.run()
```

股票实时快照
3.5.3.2

函数接口：onSnapshot

功能描述：level-1快照数据的实时订阅回调函数
输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交所 的股票 |
| period | Period | 是 | Period.snapshot.value |

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | 股票为Snapshot（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

```python
@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
```

```python
def onSnapshot(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
```

- print(period, data)

```python
sub_data.run()
```

逆回购实时快照
3.5.3.3

函数接口：onSnapshotglra

功能描述：level-1快照数据的实时订阅回调函数

输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持上交所、深交所的逆回购 代码 |
| period | Period | 是 | Period.snapshot.value |

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | 为Snapshot（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_GLRA')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

```python
@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
```

```python
def onSnapshotglra(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
```

- print(period, data)

```python
sub_data.run()
```

#### 3.5.3.4 期货实时快照

函数接口：onSnapshotfuture

功能描述：level-1快照数据的实时订阅回调函数
输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持中金所/上期所/大商所/ 郑商所/上海国际能源交易中心所 |
| period | Period | 是 | Period.snapshotfuture.value |

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | 期货为SnapshotFuture（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_FUTURE')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

```python
@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshotfuture.value)
```

```python
def onSnapshotfuture (data: Union[ad.constant.SnapshotFuture], period):
```

- print(period, data)

```python
sub_data.run()
```

实时快照
3.5.3.5 ETF

函数接口：onSnapshotetf

功能描述：level-1快照数据的实时订阅回调函数

输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持上交所、深交所的ETF |
| period | Period | 是 | Period.snapshot.value |

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | ETF为Snapshot（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

```python
@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
```

```python
def onSnapshotetf(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
```

- print(period, data)

```python
sub_data.run()
```

可转债实时快照
3.5.3.6

函数接口：onSnapshotkzz

功能描述：level-1快照数据的实时订阅回调函数

输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持上交所、深交所的可转债 |
| period | Period | 是 | Period.snapshot.value |

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | 可转债为Snapshot（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_KZZ')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

```python
@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
```

```python
def onSnapshotkzz(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
```

- print(period, data)

```python
sub_data.run()
```

港股通实时快照
3.5.3.7

函数接口：onSnapshothkt

功能描述：港股通快照数据的实时订阅回调函数

输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持上交所、深交所的可转债 |
| period | Period | 是 | Period.snapshotHKT.value |

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | 港股通为SnapshotHKT（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_HKT')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

```python
@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
```

```python
def onSnapshothkt(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
```

- print(period, data)

```python
sub_data.run()
```

#### 3.5.3.8 ETF 期权实时快照

函数接口：onSnapshotoption

功能描述：港股通快照数据的实时订阅回调函数

输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持上交所、深交所的 ETF 期权 |
| period | Period | 是 | Period.snapshotoption.value |

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | ETF期权为SnapshotOption（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
option_code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

```python
@sub_data.register(code_list=option_code_list, period=ad.constant.Period.snapshotoption.value)
```

```python
def onSnapshotoption(data: Union[ad.constant.SnapshotOption], period):
```

- print('onSnapshotoption: ', data)

```python
sub_data.run()
```

#### 3.5.3.9 实时 K 线

函数接口：OnKLine
功能描述：K线数据的实时订阅回调函数

输入参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交 |

- 所的可转债、股票、指数、ETF等品种 支持期货（中金所/上期所/大商所/郑商所/ 上海国际能源交易中心所）
- period

输出参数：入参需传入装饰器中SubscribeData.register

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| data | Object | Kline（见附录） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A ')
```

- # 实时订阅

```python
sub_data = ad.SubscribeData()
```

- # K线

```python
@sub_data.register(code_list=code_list, period=ad.constant.Period.min1.value)
```

```python
def OnKLine(data: Union[ad.constant.Kline], period):
```

- print('OnKLine: ', data)

```python
sub_data.run()
```

### 3.5.4 历史行情数据

- （1） 实例化AmazingData的MarketData，入参需交易日历
- （2） 调用MarketData的方法获取数据

历史快照
3.5.4.1

函数接口：query_snapshot

功能描述：快照数据的历史数据查询接口

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交 所的可转债、股票、指数、ETF、港股通 等、ETF期权等品种 |
| begin_date | int | 是 | 日期，填写8位的整型格式的日期，比如 20240101 |
| end_date | int | 是 | 日期，填写8位的整型格式的日期，比如 20240201 |
| begin_time | int | 否 | 时分秒毫秒的时间戳，填写8位或9位的 |

- 整型格式的日期，时占一位或两位，分占 两位，秒占两位，毫秒占三位，例如9点 整 为90000000, 17点25分为172500000
- end_time

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| snapshot_dict | dict | 指字典的key：代码 字典的value：dataframe， column为快照数据（指数为SnapshotIndex（见附录）， 股票、ETF和可转债为Snapshot（见附录）， 港股通为SnapshotHKT（见附录））， ETF期权为SnapshotOption（见附录））， index为日期（datetime） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A ')
```

```python
calendar = base_data_object.get_calendar()
```

- market_data_object=ad.MarketData(calendar)
- snapshot_dict = market_data_object.query_snapshot(code_list, begin_date=20240530, end_date=20240530)

#### 3.5.4.2 历史 K 线

函数接口：query_kline

功能描述：K线数据的实时订阅回调函数 ，支持全部周期的K线数据查询

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交 所的可转债、股票、指数、ETF等品种， 上交所、深交所的ETF期权； 支持期货（中金所/上期所/大商所/郑商所/ 上海国际能源交易中心所） |
| begin_date | int | 是 | 日期，填写8位的整型格式的日期，比如 20240101 |

| end_date | int | 是 | 日期，填写8位的整型格式的日期，比如 20240201 |
| --- | --- | --- | --- |
| period | Period | 是 | 数据周期Period（见附录） |
| begin_time | int | 否 | 时分的时间戳，填写3位或4位的整型格 式的日期，时占一位或两位，分占两位，， 例如9点整 为900, 17点25分为1725 |
| end_time | int | 否 | 时分的时间戳，填写3位或4位的整型格 式的日期，时占一位或两位，分占两位，， 例如9点整 为900, 17点25分为1725 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| kline_dict | dict | 字典的key：代码 字典的value：dataframe， column为K线数据Kline（见附录）， index为日期（datetime） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A')
```

```python
calendar = base_data_object.get_calendar()
```

- market_data_object=ad.MarketData(calendar)
- kline_dict = market_data_object.query_kline (code_list, begin_date=20240530, end_date=20240530)

财务数据
3.5.5

资产负债表
3.5.5.1

- 函数接口：get_balance_sheet
- 功能描述：获取指定股票列表的上市公司的资产负债表数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |

| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| --- | --- | --- | --- |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| balance_sheet | dict | key：code value:dataframe column为balance_sheet的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- today = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
balance_sheet = iinfo_data_object.get_balance_sheet(all_code_list)
```

balance_sheet的字段说明：

| 字段名称 | 类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | str | 证券代码 |  |
| SECURITY_NAME | str | 证券简称 |  |
| STATEMENT_TYPE | str | 报表类型 | 参看报表类型代码表 |
| REPORT_TYPE | str | 报告期名称 | 参看报告期名称 |
| REPORTING_PERIOD | str | 报告期 |  |
| ANN_DATE | str | 公告日期 |  |
| ACTUAL_ANN_DATE | str | 实际公告日期 |  |
| ACC_PAYABLE | float | 应付票据及应付账 款 |  |
| ACC_RECEIVABLE | float | 应收票据及应收账 款 |  |
| ACC_RECEIVABLES | float | 应收款项 |  |
| ACCRUED_EXP | float | 预提费用 |  |

| ACCT_PAYABLE | float | 应付账款 |
| --- | --- | --- |
| ACCT_RECEIVABLE | float | 应收账款 |
| ACT_TRADING_SEC | float | 代理买卖证券款 |
| ACT_UW_SEC | float | 代理承销证券款 |
| ADV_PREM | float | 预收保费 |
| ADV_RECEIPT | float | 预收款项 |
| AGENCY_ASSETS | float | 代理业务资产 |
| AGENCY_BUSINESS_LI AB | float | 代理业务负债 |
| ANTICIPATION_LIAB | float | 预计负债 |
| ASSET_DEP_FUNDS_O TH_FIN_INST | float | 存放同业和其它金 融机构款项 |
| BONDS_PAYABLE | float | 应付债券 |
| CAP_RESV | float | 资本公积金 |
| CAP_STOCK | float | 股本 |
| CASH_CENTRAL_BAN K_DEPOSITS | float | 现金及存放中央银 行款项 |
| CED_INSUR_CONT_RE SERVES_RCV | float | 应收分保合同准备 金 |
| CLAIMS_PAYABLE | float | 应付赔付款 |
| CLIENTS_FUND_DEPO SIT | float | 客户资金存款 |
| CLIENTS_RESERVES | float | 客户备付金 |
| CNVD_DIFF_FOREIGN_ CURR_STAT | float | 外币报表折算差额 |
| COMP_TYPE_CODE | int | 公司类型代码 |
| CONST_IN_PROC | float | 在建工程 |
| CONST_IN_PROC_TOT AL | float | 在建工程(合计)(元) |

| CONSUMP_BIO_ASSET S | float | 消耗性生物资产 |
| --- | --- | --- |
| CONT_ASSETS | float | 合同资产 |
| CONT_LIABILITIES | float | 合同负债 |
| CURRENCY_CAP | float | 货币资金 |
| CURRENCY_CODE | float | 货币代码 |
| DEBT_INV | float | 债权投资(元) |
| DEFERRED_INC_NONC UR_LIAB | float | 递延收益-非流动负 债 |
| DEFERRED_INCOME | float | 递延收益 |
| DEFERRED_TAX_ASSE TS | float | 递延所得税资产 |
| DEFERRED_TAX_LIAB | float | 递延所得税负债 |
| DEP_RECEIVED_IB_DE P | float | 吸收存款及同业存 放 |
| DEPOSIT_CAP_RECOG | float | 存出资本保证金 |
| DEPOSIT_TAKING | float | 吸收存款 |
| DEPOSITS_RECEIVED | float | 存入保证金 |
| DER_FIN_ASSETS | float | 衍生金融资产 |
| DERI_FIN_LIAB | float | 衍生金融负债 |
| DEVELOP_EXP | float | 开发支出 |
| DISPOSAL_FIX_ASSET S | float | 固定资产清理 |
| DIV_PAYABLE | float | 应付股利 |
| DIV_RECEIVABLE | float | 应收股利 |
| EMPL_PAY_PAYABLE | float | 应付职工薪酬 |
| ENGIN_MAT | float | 工程物资 |
| FIN_ASSETS_AVA_FOR _SALE | float | 可供出售金融资产 |
| FIN_ASSETS_COST_SH ARING | float | 以摊余成本计量的 金融资产 |

| FIN_ASSETS_FAIR_VAL UE | float | 以公允价值计量且 其变动计入其他综 合收益的金融资产 |
| --- | --- | --- |
| FIXED_ASSETS | float | 固定资产 |
| FIXED_ASSETS_TOTAL | float | 固定资产(合计)(元) |
| FIXED_TERM_DEPOSIT S | float | 定期存款 |
| GOODWILL | float | 商誉 |
| GUA_DEPOSITS_PAID | float | 存出保证金 |
| GUA_PLEDGE_LOANS | float | 保户质押贷款 |
| HOLD_ASSETS_FOR_S ALE | float | 持有待售的资产 |
| HOLD_TO_MTY_INV | float | 持有至到期投资 |
| INC_PLEDGE_LOAN | float | 其中:质押借款 |
| INCL_TRADING_SEAT_ FEES | float | 其中:交易席位费 |
| IND_ACCT_ASSETS | float | 独立账户资产 |
| IND_ACCT_LIAB | float | 独立账户负债 |
| INSURED_DEPOSIT_IN V | float | 保户储金及投资款 |
| INSURED_DIV_PAYABL E | float | 应付保单红利 |
| INT_RECEIVABLE | float | 应收利息 |
| INTANGIBLE_ASSETS | float | 无形资产 |
| INTEREST_PAYABLE | float | 应付利息 |
| INV | float | 存货 |
| INV_REALESTATE | float | 投资性房地产 |
| LEASE_LIABILITY | float | 租赁负债 |
| LEND_FUNDS | float | 融出资金 |
| LENDING_FUNDS | float | 拆出资金 |
| LESS_TREASURY_STK | float | 减:库存股 |

| LIA_HFS | float | 持有待售的负债 |
| --- | --- | --- |
| LIAB_DEP_FUNDS_OT H_FIN_INST | float | 同业和其它金融机 构存放款项 |
| LIFE_INSUR_RESV | float | 寿险责任准备金 |
| LOAN_CENTRAL_BAN K | float | 向中央银行借款 |
| LOANS_AND_ADVANC ES | float | 发放贷款及垫款 |
| LOANS_FROM_OTH_B ANKS | float | 拆入资金 |
| LT_DEFERRED_EXP | float | 长期待摊费用 |
| LT_EMP_COMP_PAY | float | 长期应付职工薪酬 |
| LT_EQUITY_INV | float | 长期股权投资 |
| LT_HEALTH_INSUR_RE SV | float | 长期健康险责任准 备金 |
| LT_LOAN | float | 长期借款 |
| LT_PAYABLE | float | 长期应付款 |
| LT_PAYABLE_TOTAL | float | 长期应付款(合计) (元) |
| LT_RECEIVABLES | float | 长期应收款 |
| MINORITY_EQUITY | float | 少数股东权益 |
| NOM_RISKS_PREP | float | 一般风险准备 |
| NONCUR_ASSETS_DUE _WITHIN_1Y | float | 一年内到期的非流 动资产 |
| NONCUR_LIAB_DUE_ WITHIN_1Y | float | 一年内到期的非流 动负债 |
| NOTES_PAYABLE | float | 应付票据 |
| NOTES_RECEIVABLE | float | 应收票据 |
| OIL_AND_GAS_ASSET S | float | 油气资产 |
| OTH_COMP_INCOME | float | 其他综合收益 |
| OTH_EQUITY_TOOLS | float | 其他权益工具 |

| OTH_EQUITY_TOOLS_ PRE_SHR | float | 其他权益工具:优先 股 |
| --- | --- | --- |
| OTH_NONCUR_ASSETS | float | 其他非流动资产 |
| OTHER_ASSETS | float | 其他资产 |
| OTHER_CUR_ASSETS | float | 其他流动资产 |
| OTHER_CUR_LIAB | float | 其他流动负债 |
| OTHER_DEBT_INV | float | 其他债权投资(元) |
| OTHER_EQUITY_INV | float | 其他权益工具投资 (元) |
| OTHER_LIAB | float | 其他负债 |
| OTHER_NONCUR_FIN_ ASSETS | float | 其他非流动金融资 产(元) |
| OTHER_NONCUR_LIAB | float | 其他非流动负债 |
| OTHER_PAYABLE | float | 其他应付款 |
| OTHER_PAYABLE_TOT AL | float | 其他应付款(合计) (元) |
| OTHER_RCV_TOTAL | float | 其他应收款(合计) （元） |
| OTHER_RECEIVABLE | float | 其他应收款 |
| OTHER_SUSTAIN_BON D | float | 其他权益工具:永续 债(元) |
| OUT_LOSS_RESV | float | 未决赔款准备金 |
| PAYABLE | float | 应付款项 |
| PAYABLE_FOR_REINSU RER | float | 应付分保账款 |
| PRECIOUS_METAL | float | 贵金属 |
| PREPAYMENT | float | 预付款项 |
| PROD_BIO_ASSETS | float | 生产性生物资产 |
| RCV_CED_CLAIM_RES V | float | 应收分保未决赔款 准备金 |
| RCV_CED_LIFE_INSUR _RESV | float | 应收分保寿险责任 准备金 |

| RCV_CED_LT_HEALTH _INSUR_RESV | float | 应收分保长期健康 险责任准备金 |
| --- | --- | --- |
| RCV_CED_UNEARNED _PREM_RESV | float | 应收分保未到期责 任准备金 |
| RCV_FINANCING | float | 应收款项融资 |
| RCV_INV | float | 应收款项类投资 |
| RECEIVABLE_PREM | float | 应收保费 |
| RED_MON_CAP_FOR_S ALE | float | 买入返售金融资产 |
| REINSURANCE_ACC_R CV | float | 应收分保账款 |
| RSRV_FUND_INSUR_C ONT | float | 保险合同准备金 |
| SELL_REPO_FIN_ASSE TS | float | 卖出回购金融资产 款 |
| SERVICE_CHARGE_CO MM_PAYABLE | float | 应付手续费及佣金 |
| SETTLE_FUNDS | float | 结算备付金 |
| SPE_ASSETS_BAL_DIF F | float | 资产差额(特殊报表 科目) |
| SPE_CUR_ASSETS_DIF F | float | 流动资产差额(特殊 报表科目) |
| SPE_CUR_LIAB_DIFF | float | 流动负债差额(特殊 报表科目) |
| SPE_LIAB_BAL_DIFF | float | 负债差额(特殊报表 科目) |
| SPE_LIAB_EQUITY_BA L_DIFF | float | 负债及股东权益差 额(特殊报表项目) |
| SPE_NONCUR_ASSETS _DIFF | float | 非流动资产差额(特 殊报表科目) |
| SPE_NONCUR_LIAB_DI FF | float | 非流动负债差额(特 殊报表科目) |
| SPE_SHARE_EQUITY_B AL_DIFF | float | 股东权益差额(特殊 报表科目) |

| SPECIAL_PAYABLE | float | 专项应付款 |
| --- | --- | --- |
| SPECIAL_RESV | float | 专项储备 |
| ST_BONDS_PAYABLE | float | 应付短期债券 |
| ST_BORROWING | float | 短期借款 |
| ST_FIN_PAYABLE | float | 应付短期融资款 |
| SUBR_RCV | float | 应收代位追偿款 |
| SURPLUS_RESV | float | 盈余公积金 |
| TAX_PAYABLE | float | 应交税费 |
| TOT_ASSETS_BAL_DIF F | float | 资产差额(合计平衡 项目) |
| TOT_CUR_ASSETS_DIF F | float | 流动资产差额(合计 平衡项目) |
| TOT_CUR_LIAB_DIFF | float | 流动负债差额(合计 平衡项目) |
| TOT_LIAB_BAL_DIFF | float | 负债差额(合计平衡 项目) |
| TOT_LIAB_EQUITY_BA L_DIFF | float | 负债及股东权益差 额(合计平衡项目) |
| TOT_NONCUR_ASSETS | float | 非流动资产合计 |
| TOT_NONCUR_ASSETS _DIFF | float | 非流动资产差额(合 计平衡项目) |
| TOT_NONCUR_LIAB_D IFF | float | 非流动负债差额(合 计平衡项目) |
| TOT_SHARE | float | 期末总股本 |
| TOT_SHARE_EQUITY_ BAL_DIFF | float | 股东权益差额(合计 平衡项目) |
| TOT_SHARE_EQUITY_ EXCL_MIN_INT | float | 股东权益合计(不含 少数股东权益) |
| TOT_SHARE_EQUITY_I NCL_MIN_INT | float | 股东权益合计(含少 数股东权益) |
| TOTAL_ASSETS | float | 资产总计 |
| TOTAL_CUR_ASSETS | float | 流动资产合计 |

| TOTAL_CUR_LIAB | float | 流动负债合计 |
| --- | --- | --- |
| TOTAL_LIAB | float | 负债合计 |
| TOTAL_LIAB_SHARE_E QUITY | float | 负债及股东权益总 计 |
| TOTAL_NONCUR_LIAB | float | 非流动负债合计 |
| TRADING_FIN_LIAB | float | 交易性金融负债 |
| TRADING_FINASSETS | float | 交易性金融资产 |
| UNAMORTIZED_EXP | float | 待摊费用 |
| UNCONFIRMED_INV_L OSS | float | 未确认的投资损失 |
| UNDISTRIBUTED_PRO | float | 未分配利润 |
| UNEARNED_PREM_RE SV | float | 未到期责任准备金 |
| USE_RIGHT_ASSETS | float | 使用权资产 |

现金流量表
3.5.5.2

- 函数接口：get_cash_flow
- 功能描述：获取指定股票列表的上市公司的现金流量表数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| cash_flow | dict | key：code value:dataframe column为cash_flow的字段 index为序号（无意义） |

| i i t | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
cash_flow = iinfo_data_object.get_cash_flow (all_code_list)
```

cash_flow 的字段说明：

| 字段名称 | 类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | str | 证券代码 |  |
| SECURITY_NAM | strE | 证券简称 |  |
| STATEMENT_TYP E | str | 报表类型 | 参看报表类 型代码表 |
| REPORT_TYPE | str | 报告期名称名称 | 参看报告期 |
| REPORTING_PERI OD | str | 报告期 |  |
| ANN_DATE | str | 公告日期 |  |
| ACTUAL_ANN_D ATE | str | 实际公告日期 |  |
| ABSORB_CASH_ | doubleRECP_INV | 吸收投资收到的现金 |  |
| AMORT_INTAN_ | doubleASSETS | 无形资产摊销 |  |
| AMORT_LT_DEFE | doubleRRED_EXP | 长期待摊费用摊销 |  |
| BEG_BAL_CASH_ | doubleCASH_EQU | 期初现金及现金等价物余额 |  |
| CASH_END_BAL | double | 现金的期末余额 |  |

- STATEMENT_TYP
- E

- 参看报表类
- 型代码表

- REPORTING_PERI
- OD

- ACTUAL_ANN_D
- ATE

| CASH_FOR_CHA | doubleRGE | 支付手续费的现金 |
| --- | --- | --- |
| CASH_PAID_INSU | doubleR_POLICY | 支付保单红利的现金 |
| CASH_PAID_INV | double | 投资支付的现金 |
| CASH_PAID_PUR _CONST_FIOLTA | double | 购建固定资产、无形资产和其他长期 资产支付的现金 |
| CASH_PAY_CLAI MS_OIC | double | 支付原保险合同赔付款项的现金 |
| CASH_PAY_DIST_ DIV_PRO_INT | double | 分配股利、利润或偿付利息支付的现 金 |
| CASH_PAY_EMPL OYEE | double | 支付给职工以及为职工支付的现金 |
| CASH_PAY_FOR_ | doubleDEBT | 偿还债务支付的现金 |
| CASH_PAY_GOO DS_SERVICES | double | 购买商品、接受劳务支付的现金 |
| CASH_RECE_BO | doubleRROW | 取得借款收到的现金 |
| CASH_RECE_ISS | doubleUE_BONDS | 发行债券收到的现金 |
| CASH_RECP_INV | double_INCOME | 取得投资收益收到的现金 |
| CASH_RECP_PRE M_OIC | double | 收到原保险合同保费取得的现金 |
| CASH_RECP_REC | doubleOV_INV | 收回投资收到的现金 |
| CASH_RECP_SG_ AND_RS | double | 销售商品、提供劳务收到的现金 |

- CASH_PAID_PUR
- _CONST_FIOLTA

- 购建固定资产、无形资产和其他长期
- 资产支付的现金

- CASH_PAY_CLAI
- MS_OIC

- CASH_PAY_DIST_
- DIV_PRO_INT

- 分配股利、利润或偿付利息支付的现
- 金

- CASH_PAY_EMPL
- OYEE

- CASH_PAY_GOO
- DS_SERVICES

- CASH_RECP_PRE
- M_OIC

- CASH_RECP_SG_
- AND_RS

| COMP_TYPE_CO DE | str | 公司类型代码2：银行 3：保险4：证券 | 1：非金融类 |
| --- | --- | --- | --- |
| CONV_CORP_BO | doubleNDS_DUE_WITHIN_1Y | 一年内到期的可转换公司债券 |  |
| CONV_DEBT_INT | doubleO_CAP | 债务转为资本 |  |
| CREDIT_IMPAIR_ | doubleLOSS | 信用减值损失 |  |
| CURRENCY_COD | strE | 货币代码 |  |
| DECR_DEFE_INC | double_TAX_ASSETS | 递延所得税资产减少 |  |
| DECR_DEFERRE | doubleD_EXPENSE | 待摊费用减少 |  |
| DECR_INVENTOR | doubleY | 存货的减少 |  |
| DECR_OPERA_RE | doubleCEIVABLE | 经营性应收项目的减少 |  |
| DEPRE_FA_OGA_ PBA | double | 固定资产折旧、油气资产折耗、生产 性生物资产折旧 |  |
| EFF_FX_FLUC_C | doubleASH | 汇率变动对现金的影响 |  |
| END_BAL_CASH_ | doubleCASH_EQU | 期末现金及现金等价物余额 |  |
| FINANCIAL_EXP | double | 财务费用 |  |
| FIXED_ASSETS_F | doubleIN_LEASE | 融资租入固定资产 |  |
| FREE_CASH_FLO | doubleW | 企业自由现金流量 |  |

- COMP_TYPE_CO
- DE

- DEPRE_FA_OGA_
- PBA

- 固定资产折旧、油气资产折耗、生产
- 性生物资产折旧

| INCL_CASH_REC P_SAIMS | double | 其中:子公司吸收少数股东投资收到 的现金 |
| --- | --- | --- |
| INCL_DIV_PRO_P AID_SMS | double | 其中:子公司支付给少数股东的股利、 利润 |
| INCR_ACCRUED_ | doubleEXP | 预提费用增加 |
| INCR_DEFE_INC_ | doubleTAX_LIAB | 递延所得税负债增加 |
| INCR_OPERA_PA | doubleYABLE | 经营性应付项目的增加 |
| IND_NET_CASH_ | doubleFLOWS_OPERA_ACT | 间接法-经营活动产生的现金流量净 额 |
| IND_NET_INCR_ CASH_AND_EQU | double | 间接法-现金及现金等价物净增加额 |
| INV_LOSS | double | 投资损失 |
| IS_CALCULATIO | intN | 是否计算报表 |
| LESS_OPEN_BAL | double_CASH | 减:现金的期初余额 |
| LESS_OPEN_BAL | double_CASH_EQU | 减:现金等价物的期初余额 |
| LOSS_DISP_FIOL TA | double | 处置固定、无形资产和其他长期资产 的损失 |
| LOSS_FAIRVALU | doubleE_CHG | 公允价值变动损失 |
| LOSS_FIXED_ASS | doubleETS | 固定资产报废损失 |
| NET_CASH_FLO WS_FIN_ACT | double | 筹资活动产生的现金流量净额 |

- INCL_CASH_REC
- P_SAIMS

- 其中:子公司吸收少数股东投资收到
- 的现金

- INCL_DIV_PRO_P
- AID_SMS

- 其中:子公司支付给少数股东的股利、
- 利润

- 间接法-经营活动产生的现金流量净
- 额

- IND_NET_INCR_
- CASH_AND_EQU

- LOSS_DISP_FIOL
- TA

- 处置固定、无形资产和其他长期资产
- 的损失

- NET_CASH_FLO
- WS_FIN_ACT

| NET_CASH_FLO WS_INV_ACT | double | 投资活动产生的现金流量净额 |
| --- | --- | --- |
| NET_CASH_FLO WS_OPERA_ACT | double | 经营活动产生的现金流量净额 |
| NET_CASH_PAID _SOBU | double | 取得子公司及其他营业单位支付的现 金净额 |
| NET_CASH_REC_ SEC | double | 代理买卖证券收到的现金净额 |
| NET_CASH_RECP _DISP_FIOLTA | double | 处置固定资产、无形资产和其他长期 资产收回的现金净额 |
| NET_CASH_RECP _DISP_SOBU | double | 处置子公司及其他营业单位收到的现 金净额 |
| NET_CASH_RECP | double_REINSU_BUS | 收到再保业务现金净额 |
| NET_INCR_BORR | double_FUND | 拆入资金净增加额 |
| NET_INCR_BORR _OFI | double | 向其他金融机构拆入资金净增加额 |
| NET_INCR_CASH | double_AND_CASH_EQU | 现金及现金等价物净增加额 |
| NET_INCR_CUS_ | doubleLOAN_ADV | 客户贷款及垫款净增加额 |
| NET_INCR_DEP_ CB_IB | double | 存放央行和同业款项净增加额 |
| NET_INCR_DEP_ CUS_AND_IB | double | 客户存款和同业存放款项净增加额 |

- NET_CASH_FLO
- WS_INV_ACT

- NET_CASH_FLO
- WS_OPERA_ACT

- NET_CASH_PAID
- _SOBU

- 取得子公司及其他营业单位支付的现
- 金净额

- NET_CASH_REC_
- SEC

- NET_CASH_RECP
- _DISP_FIOLTA

- 处置固定资产、无形资产和其他长期
- 资产收回的现金净额

- NET_CASH_RECP
- _DISP_SOBU

- 处置子公司及其他营业单位收到的现
- 金净额

- NET_INCR_BORR
- _OFI

- NET_INCR_DEP_
- CB_IB

- NET_INCR_DEP_
- CUS_AND_IB

| NET_INCR_DISM | doubleANTLE_CAP | 拆出资金净增加额 |
| --- | --- | --- |
| NET_INCR_DISP_ FAAS | double | 处置可供出售金融资产净增加额 |
| NET_INCR_DISP_ TFA | double | 处置交易性金融资产净增加额 |
| NET_INCR_INSU | doubleRED_SAVE | 保户储金净增加额 |
| NET_INCR_INT_A | doubleND_CHARGE | 收取利息和手续费净增加额 |
| NET_INCR_LOAN | doubleS_CENTRAL_BANK | 向中央银行借款净增加额 |
| NET_INCR_PLED | doubleGE_LOAN | 质押贷款净增加额 |
| NET_INCR_REPU | double_BUS_FUND | 回购业务资金净增加额 |
| NET_PROFIT | double | 净利润 |
| OTH_CASH_PAY_ INV_ACT | double | 支付其他与投资活动有关的现金 |
| OTH_CASH_PAY_ OPERA_ACT | double | 支付其他与经营活动有关的现金 |
| OTH_CASH_RECP _INV_ACT | double | 收到其他与投资活动有关的现金 |
| OTHER_ASSETS_ | doubleIMPAIR_LOSS | 其他资产减值损失 |
| OTHER_CASH_PA Y_FIN_ACT | double | 支付其他与筹资活动有关的现金 |
| OTHER_CASH_R ECP_FIN_ACT | double | 收到其他与筹资活动有关的现金 |

- NET_INCR_DISP_
- FAAS

- NET_INCR_DISP_
- TFA

- OTH_CASH_PAY_
- INV_ACT

- OTH_CASH_PAY_
- OPERA_ACT

- OTH_CASH_RECP
- _INV_ACT

- OTHER_CASH_PA
- Y_FIN_ACT

- OTHER_CASH_R
- ECP_FIN_ACT

| OTHER_CASH_R ECP_OPER_ACT | double | 收到其他与经营活动有关的现金 |
| --- | --- | --- |
| OTHERS | double | 其他（废弃） |
| PAY_ALL_TAX | double | 支付的各项税费 |
| PLUS_ASSETS_D | doubleEPRE_PREP | 加:资产减值准备 |
| PLUS_END_BAL_ | doubleCASH_EQU | 加:现金等价物的期末余额 |
| RECP_TAX_REFU | doubleND | 收到的税费返还 |
| SPE_BAL_CASH_I NFLOW_FIN_ACT | double | 筹资活动现金流入差额 |
| SPE_BAL_CASH_I | doubleNFLOW_INV_ACT | 投资活动现金流入差额 |
| SPE_BAL_CASH_I | doubleNFLOW_OPERA_ACT | 经营活动现金流入差额 |
| SPE_BAL_CASH_ | doubleOUTFLOW_FIN | 筹资活动现金流出差额 |
| SPE_BAL_CASH_ | doubleOUTFLOW_INV | 投资活动现金流出差额 |
| SPE_BAL_CASH_ | doubleOUTFLOW_OPERA | 经营活动现金流出差额 |
| SPE_BAL_NETCA | doubleSH_INC_DIFF_IND | 间接法-现金净增加额差额 |
| SPE_BAL_NETCA | doubleSH_INCR_DIFF | 现金净增加额差额 |
| SPE_BAL_NETCA SH_OPERA_IND | double | 间接法-经营活动现金流量净额差额 |

- OTHER_CASH_R
- ECP_OPER_ACT

- SPE_BAL_CASH_I
- NFLOW_FIN_ACT

- SPE_BAL_NETCA
- SH_OPERA_IND

| TOT_BAL_CASH_ | doubleINFLOW_FIN_ACT | 筹资活动现金流入差额 |
| --- | --- | --- |
| TOT_BAL_CASH_ | doubleINFLOW_INV_ACT | 投资活动现金流入差额 |
| TOT_BAL_CASH_ | doubleINFLOW_OPERA_ACT | 经营活动现金流入差额 |
| TOT_BAL_CASH_ OUTFLOW_FIN | double | 筹资活动现金流出差额 |
| TOT_BAL_CASH_ OUTFLOW_INV | double | 投资活动现金流出差额 |
| TOT_BAL_CASH_ | doubleOUTFLOW_OPERA | 经营活动现金流出差额 |
| TOT_BAL_NETCA SH_FLOW_FIN | double | 筹资活动产生的现金流量净额差额 |
| TOT_BAL_NETCA SH_FLOW_INV | double | 投资活动产生的现金流量净额差额 |
| TOT_BAL_NETCA | doubleSH_FLOW_OPERA | 经营活动产生的现金流量净额差额 |
| TOT_BAL_NETCA | doubleSH_INC_DIFF_IND | 间接法-现金净增加额差额 |
| TOT_BAL_NETCA | doubleSH_INCR_DIFF | 现金净增加额差额 |
| TOT_BAL_NETCA SH_OPERA_IND | double | 间接法-经营活动现金流量净额差额 |
| TOT_CASH_INFL | doubleOW_FIN_ACT | 筹资活动现金流入小计 |
| TOT_CASH_INFL | doubleOW_INV_ACT | 投资活动现金流入小计 |

- TOT_BAL_CASH_
- OUTFLOW_FIN

- TOT_BAL_CASH_
- OUTFLOW_INV

- TOT_BAL_NETCA
- SH_FLOW_FIN

- TOT_BAL_NETCA
- SH_FLOW_INV

- TOT_BAL_NETCA
- SH_OPERA_IND

| TOT_CASH_INFL | doubleOW_OPER_ACT | 经营活动现金流入小计 |
| --- | --- | --- |
| TOT_CASH_OUTF LOW_FIN_ACT | double | 筹资活动现金流出小计 |
| TOT_CASH_OUTF LOW_INV_ACT | double | 投资活动现金流出小计 |
| TOT_CASH_OUTF | doubleLOW_OPERA_ACT | 经营活动现金流出小计 |
| UNCONFIRMED_I | doubleNV_LOSS | 未确认投资损失 |
| USE_RIGHT_ASS | doubleET_DEP | 使用权资产折旧 |

- TOT_CASH_OUTF
- LOW_FIN_ACT

- TOT_CASH_OUTF
- LOW_INV_ACT

利润表
3.5.5.3

- 函数接口：get_income
- 功能描述：获取指定股票列表的上市公司的利润表数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| income | dict | key：code value:dataframe column为income的字段 index为序号（无意义） |

| i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
i t i | ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
ncome = iinfo_data_object.get_income (all_code_list)
```

income的字段说明：

| 字段名称 | 类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | str | 证券代码 |  |
| SECURITY_NAME | str | 证券简称 |  |
| STATEMENT_TYP | str | 报表类型E | 参看报表类型代码表 |
| REPORT_TYPE | str | 报告期名称 | 参看报告期名称 |
| REPORTING_PERI | strOD | 报告期 |  |
| ANN_DATE | str | 公告日期 |  |
| ACTUAL_ANN_DA | strTE | 实际公告日期 |  |
| AMORT_COST_FI N_ASSETS_EAR | float金融资产终止确认收益 | 以摊余成本计量的 |  |
| ANN_DATE | str | 公告日期 |  |
| BASIC_EPS | float | 基本每股收益 |  |
| BEG_UNDISTRIBU | floatTED_PRO | 年初未分配利润 |  |
| CAPITALIZED_CO | floatM_STOCK_DIV | 转作股本的普通股股利 |  |
| COMMENTS | str | 备注 |  |
| COMMON_STOCK | float_DIV_PAYABLE | 应付普通股股利 |  |
| COMP_TYPE_COD E | str | 公司类型代码 | 1：非金融类2：银行3： 保险4：证券 |

- AMORT_COST_FI
- N_ASSETS_EAR

- COMP_TYPE_COD
- E

- 1：非金融类2：银行3
- 保险4：证券

| CONTINUED_NET | float_OPERA_PRO | 持续经营净利润 |
| --- | --- | --- |
| CREDIT_IMPAIR_L | floatOSS | 信用减值损失 |
| CURRENCY_CODE | str | 货币代码 |
| DILUTED_EPS | float | 稀释每股收益 |
| DISTRIBUTIVE_PR | floatO | 可分配利润 |
| DISTRIBUTIVE_PR | floatO_SHAREHOLDER | 可供股东分配的利润 |
| DIV_EXP_INSUR | float | 保户红利支出 |
| EBIT | float | 息税前利润 |
| EBITDA | float润 | 息税折旧摊销前利 |
| EMPLOYEE_WELF | floatARE | 职工奖金福利 |
| END_NET_OPERA | float_PRO | 终止经营净利润 |
| EXT_INSUR_CONT | float_RSRV | 提取保险责任准备金 |
| EXT_UNEARNED_ | floatPREM_RES | 提取未到期责任准备金 |
| FIN_EXP_INT_EXP | float用 | 财务费用:利息费 |
| FIN_EXP_INT_INC | float入 | 财务费用:利息收 |
| GAIN_DISPOSAL_ | floatASSETS | 资产处置收益 |
| HANDLING_CHRG | float_COMM_FEE | 手续费及佣金收入 |
| INCL_INC_INV_JV _ENTP | float和合营企业的投资收益 | 其中:对联营企业 |
| INCL_LESS_LOSS_ | float | 其中:减:非流动资 |

- INCL_INC_INV_JV
- _ENTP

| DISP_NCUR_ASSE | 产处置净损失 |
| --- | --- |
| T |  |
| INCL_REINSUR_P | floatREM_INC |
| INCOME_TAX | float |
| INSUR_EXP | float |
| INSUR_PREM | float |
| INTEREST_INC | float |
| IS_CALCULATION | float |
| LESS_ADMIN_EXP | float |
| LESS_AMORT_CO | floatMPEN_EXP |
| LESS_AMORT_INS | floatUR_CONT_RSRV |
| LESS_AMORT_REI | floatNSUR_EXP |
| LESS_ASSETS_IMP | floatAIR_LOSS |
| LESS_BUS_TAX_S | floatURCHARGE |
| LESS_FIN_EXP | float |
| LESS_HANDLING_ | floatCHRG_COMM_FEE |
| LESS_INTEREST_E | floatXP |
| LESS_NON_OPER | floatA_EXP |
| LESS_OPERA_COS | floatT |
| LESS_REINSUR_P | floatREM |
| LESS_SELLING_E | floatXP |

- 减:手续费及佣金
- 支出

| MARKET_CODE | str | 证券代码 |
| --- | --- | --- |
| MIN_INT_INC | float | 少数股东损益 |
| NET_EXPOSURE_ | floatHEDGING_GAIN | 净敞口套期收益 |
| NET_HANDLING_ | floatCHRG_COMM_FEE | 手续费及佣金净收 入 |
| NET_INC_EC_ASS | floatET_MGMT_BUS | 受托客户资产管理业务净收入 |
| NET_INC_SEC_BR | floatOK_BUS | 代理买卖证券业务净收入 |
| NET_INC_SEC_UW | float_BUS | 证券承销业务净收入 |
| NET_INTEREST_I | floatNC | 利息净收入 |
| NET_PRO_AFTER_ DED_NR_GL | float后净利润（扣除少数股东损益） | 扣除非经常性损益 |
| NET_PRO_AFTER_ DED_NR_GL_COR | float后的净利润(财务重要指标(更正前)) | 扣除非经常性损益 |
| NET_PRO_EXCL_ | floatMIN_INT_INC | 净利润(不含少数股东损益) |
| NET_PRO_INCL_M | floatIN_INT_INC | 净利润(含少数股东损益) |
| NET_PRO_UNDER | float_INT_ACC_STA | 国际会计准则净利润 |
| OPERA_EXP | float | 营业支出 |
| OPERA_PROFIT | float | 营业利润 |
| OPERA_REV | float | 营业收入 |
| OTH_ASSETS_IMP | floatAIR_LOSS | 其他资产减值损失 |
| OTH_BUS_COST | float | 其他业务成本 |
| OTH_BUS_INC | float | 其他业务收入 |

- 手续费及佣金净收
- 入

- NET_PRO_AFTER_
- DED_NR_GL

- NET_PRO_AFTER_
- DED_NR_GL_COR

| OTH_COMPRE_IN | floatC | 其他综合收益 |
| --- | --- | --- |
| OTH_INCOME | float | 其他收益 |
| OTH_NET_OPERA | float_INC | 其他经营净收益 |
| PLUS_NET_FX_IN | floatC | 加:汇兑净收益 |
| PLUS_NET_GAIN_ | floatCHG_FV | 加:公允价值变动净收益 |
| PLUS_NET_INV_I | floatNC | 加:投资净收益 |
| PLUS_NON_OPER | floatA_REV | 加:营业外收入 |
| PLUS_OTH_NET_B | floatUS_INC | 加:其他业务净收益 |
| PREFERRED_SHA | floatRE_DIV_PAYABLE | 应付优先股股利 |
| PREM_BUS_INC | float | 保费业务收入 |
| RD_EXP | float | 研发费用 |
| REINSURANCE_E | floatXP | 分保费用 |
| REPORTING_PERI | strOD | 报告期 |
| SECURITY_NAME | str | 证券简称 |
| SPE_BAL_NET_PR | floatO_MARG | 净利润差额(特殊报表科目) |
| SPE_BAL_OPERA_ | floatPRO_MARG | 营业利润差额(特殊报表科目) |
| SPE_BAL_TOT_OP | floatERA_COST_DIF | 营业总成本差额(特殊报表科目) |
| SPE_BAL_TOT_OP | floatERA_INC_DIF | 营业总收入差额(特殊报表科目) |
| SPE_BAL_TOT_PR | floatO_MARG | 利润总额差额(特殊报表科目) |

| SPE_TOT_OPERA_ | strCOST_DIF_STATE | 营业总成本差额说明(特殊报表科目) |
| --- | --- | --- |
| SPE_TOT_OPERA_ | strINC_DIF_STATE | 营业总收入差额说明(特殊报表科目) |
| SURR_VALUE | float | 退保金 |
| TOT_BAL_NET_PR | floatO_MARG | 净利润差额(合计平衡项目) |
| TOT_BAL_OPERA | float_PRO_MARG | 营业利润差额(合计平衡项目) |
| TOT_BAL_TOT_PR | floatO_MARG | 利润总额差额(合计平衡项目) |
| TOT_COMPEN_EX | floatP | 赔付总支出 |
| TOT_COMPRE_IN | floatC | 综合收益总额 |
| TOT_COMPRE_IN | floatC_MIN_SHARE | 综合收益总额(少数股东) |
| TOT_COMPRE_IN | floatC_PARENT_COMP | 综合收益总额(母公司) |
| TOT_OPERA_COS | floatT | 营业总成本 |
| TOT_OPERA_COS | floatT2 | 营业总成本2 |
| TOT_OPERA_REV | float | 营业总收入 |
| TOTAL_PROFIT | float | 利润总额 |
| TRANSFER_HOUSI | floatNG_REVO_FUNDS | 住房周转金转入 |
| TRANSFER_OTHE | floatRS | 其他转入 |
| TRANSFER_SURP | floatLUS_RESERVE | 盈余公积转入 |
| UNCONFIRMED_I | floatNV_LOSS | 未确认投资损失 |
| WITHDRAW_ANY | float | 提取任意盈余公积 |

| _SURPLUS_RESV | 金 |
| --- | --- |
| WITHDRAW_ENT_ | floatDEVELOP_FUND |
| WITHDRAW_LEG_ | floatPUB_WEL_FUND |
| WITHDRAW_LEG_ | floatSURPLUS |
| WITHDRAW_RESV | float_FUND |

业绩快报
3.5.5.4

- 函数接口：get_profit_express
- 功能描述：获取指定股票列表的上市公司的业绩快报数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| profit_express | dataframe | column为profit_express的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- today = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

| p | end_date=today) |

```python
rofit_express = iinfo_data_object.get_profit_express (all_code_list)
```

的字段说明：
profit_express

| 参数 | 数据类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | str | 证券代码 |  |
| REPORTING_PERI | str | 报告期OD | 报告内容记录的截止时间点，报告成果的时期 |
| ANN_DATE | str | 公告日期段的事件，首次披露该事件的日期 | 公告发布当天的日期；有多个阶 |
| ACTUAL_ANN_D | str | 实际公告日 | 实际数据来源公告的日期；更正 |
| ATE | 期 | 发生公告的日期 |  |
| TOTAL_ASSETS | float64 | 总资产(元)经济利益的全部资产 | 指经济实体拥有或控制的能带来 |
| NET_PRO_EXCL_ | float64 | 净利润(元)MIN_INT_INC | 企业合并净利润中归属于母公司股东所有的那部分利润 |
| TOT_OPERA_REV | float64 | 营业总收入 (元)让渡资产使用权等日常业务过程形成的经济利益的总流入 | 企业从事销售商品、提供劳务和 |
| TOTAL_PROFIT | float64 | 利 润 总 额(元) | 企业一定时期内的纯收入扣除应交纳后的余额 |
| OPERA_PROFIT | float64 | 营 业 利 润(元) | 企业在其全部销售业务中实现的利润 |
| EPS_BASIC | float64 | 每股收益- 基本(元)净利润，除以发行在外普通股的加权平均数计算得到的每股收益 | 企业按照属于普通股股东的当期 |
| TOT_SHARE_EQU _EXCL_MIN_INT | float64 | 股东权益合计(不含少数 股 东 权益)(元) | 公司集团的所有者权益中归属于 母公司所有者权益的部分 |
| IS_AUDIT | float64 | 是否审计 | 1:是 0：否 |
| ROE_WEIGHTED | float64 | 净资产收益 率-加权(%)的一个动态指标，反应企业净资产创造利润的能力 | 经营期间净资产赚取利润的结果 |
| LAST_YEAR_REV | float64 | 去年同期修ISED_NET_PRO | 元正后净利润 |

- 营业总收入
- (元)

- 每股收益-
- 基本(元)

- TOT_SHARE_EQU
- _EXCL_MIN_INT

- 公司集团的所有者权益中归属于
- 母公司所有者权益的部分

- 净资产收益
- 率-加权(%)

| PERFORMANCE_ | str | 业绩简要说SUMMARY | 针对业绩快报的简单说明明 |
| --- | --- | --- | --- |
| NET_ASSET_PS | float64 | 每股净资产 | 元 |
| MEMO | str | 备注 | 附加的注解说明 |
| YOY_GR_GROSS_ | float64PRO | 同比增长率:营业利润 |  |
| YOY_GR_GROSS_ | float64REV | 同比增长率:营业总收入 |  |
| YOY_GR_NET_PR OFIT_PARENT | float64归属母公司股东的净利润 | 同比增长率: |  |
| YOY_GR_TOT_PR | float64O | 同比增长率:利润总额 |  |
| YOY_ID_WAROE | float64权平均净资产收益率 | 同比增减:加 |  |
| YOY_GR_EPS_BA SIC | float64基本每股收益 | 同比增长率: |  |
| GROWTH_RATE_ EQUITY | float64率:归属母公司的股东权益 | 比年初增长 |  |
| GROWTH_RATE_ | float64ASSETS | 比年初增长率:总资产 |  |
| GROWTH_RATE_ NAPS | float64率:归属于母公司股东的每股净资产 | 比年初增长 |  |
| LAST_YEAR_TOT | float64 | 去年同期营_OPERA_REV | 元业总收入 |
| LAST_YEAR_TOT | float64 | 去年同期利AL_PROFIT | 元润总额 |
| LAST_YEAR_OPE | float64 | 去年同期营RA_PRO | 元业利润 |

- YOY_GR_NET_PR
- OFIT_PARENT

- YOY_GR_EPS_BA
- SIC

- GROWTH_RATE_
- EQUITY

- GROWTH_RATE_
- NAPS

| LAST_YEAR_EPS | float64 | 去年同期每_DILUTED | 元股收益 |
| --- | --- | --- | --- |
| LAST_YEAR_NET | float64 | 去年同期净_PROFIT | 元利润 |
| INITIAL_NET_AS | float64 | 期初每股净SET_PS | 元资产 |
| INITIAL_NET_AS | float64 | 期初净资产SETS | 元 |

业绩预告
3.5.5.5

- 函数接口：get_profit_notice
- 功能描述：获取指定股票列表的上市公司的业绩预告数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- profit_notice

| i i t | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
profit_notice = iinfo_data_object.get_profit_notice (all_code_list)
```

的字段说明：
profit_notice

| 参数 | 数据类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | str | 证券代码 |  |
| SECURITY_NAME | str | 证券简称 |  |
| P_TYPECODE | str | 业绩预告类型代 码2：略减3：略增4：扭亏5：其他6：首亏7：续亏8：续盈9：预减10：预增11：持平 | 1：不确定 |
| REPORTING_PERI | str | 报告期OD | 分为年度、半年度、季度 |
| ANN_DATE | str | 公告日期 | 公告发布当天的日期 |
| P_CHANGE_MAX | float64 | 预告净利润变动幅度上限（%） | 对于净利润金额同比变动幅度预计的最高值 |
| P_CHANGE_MIN | float64 | 预告净利润变动幅度下限（%） | 对于净利润金额同比变动幅度预计的最低值 |
| NET_PROFIT_MA | float64 | 预告净利润上限 | 对于净利润金额预计的最高 |
| X | （万元） | 值 |  |
| NET_PROFIT_MIN | float64 | 预告净利润下限（万元） | 对于净利润金额预计的最低值 |
| FIRST_ANN_DAT | str | 首次公告日E | 首次披露本报告期业绩预告内容的公告日期 |
| P_NUMBER | float64 | 公布次数的披露次数 | 同一报告期的业绩预告公告 |
| P_REASON | str | 业绩变动原因 |  |
| P_SUMMARY | str | 业绩预告摘要 |  |
| P_NET_PARENT_F | float64 | 上年同期归母净 | 业绩预告中直接公布的上年 |
| IRM | 利润 | 同期归母净利润 |  |
| REPORT_TYPE | str | 报告期名称 | 参看报告期名称 |

- 业绩预告类型代
- 码

股东股本数据
3.5.6

十大股东数据
3.5.6.1

- 函数接口：get_share_holder
- 功能描述：获取指定股票列表的上市公司的十大股东数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- share_holder

| i i t | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
share_holder = iinfo_data_object.get_share_holder (all_code_list)
```

的字段说明：
share_holder

| 参数 | 数据类 | 字段说明型 | 备注 |
| --- | --- | --- | --- |
| ANN_DATE | str | 公告日期, |  |
| MARKET_CODE | str | 证券代码 |  |
| HOLDER_ENDDATE | str | 到期日期 |  |
| HOLDER_TYPE | int | 股东类别 | 10:十大股东 |

- 20:流通股前十大股东
- QTY_NUM
- HOLDER_NAME
- HOLDER_HOLDER_C
- HOLDER_QUANTITY,
- HOLDER_PCT
- HOLDER_SHARECAT EGORYNAME
- FLOAT_QTY

- HOLDER_SHARECAT
- EGORYNAME

股东户数
3.5.6.2

- 函数接口：get_holder_num
- 功能描述：获取指定股票列表的上市公司的股东户数数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- holder_num

| i a i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
d.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
t | base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
holder_num = iinfo_data_object.get_holder_num (all_code_list)
```

holder_num 的字段说明：

| 参数 | 数据类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 证券代码 |
| ANN_DT | string | 公告日期 |
| HOLDER_ENDDATE | string | 股东户数统计的截止日期 |
| HOLDER_TOTAL_NUM | float | A股、B股、H股、境外股的总户数 |
| HOLDER_NUM | float | A股股东户数 |

股本结构
3.5.6.3

- 函数接口：get_equity_structure
- 功能描述：获取指定股票列表的上市公司的股本结构数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- equity_structu re

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
| calendar = base_data_object.get_calendar()                                                                    |  |        |        |  |          |            |  |
```

- today = calendar[-1]

```python
| all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101, |  |        |        |  |          |            |  |
```

- end_date=today)

```python
| equity_structure = iinfo_data_object.get_equity_structure (all_code_list)                                      |  |        |        |  |          |            |  |
```

- equity_structure 的字段说明：
- 字段名称
- MARKET_CODE
- ANN_DATE
- CHANGE_DATE
- SHARE_CHANGE_REA
- EX_CHANGE_DATE
- CURRENT_SIGN
- IS_VALID
- TOT_SHARE
- FLOAT_SHARE
- FLOAT_A_SHARE
- FLOAT_B_SHARE
- FLOAT_HK_SHARE
- FLOAT_OS_SHARE
- TOT_TRADABLE_SHA
- RTD_A_SHARE_INST
- RTD_A_SHARE_DOME SNP
- RTD_SHARE_SENIOR
- RTD_A_SHARE_FOREI
- RTD_A_SHARE_FORJ
- RTD_A_SHARE_FORN
- RESTRICTED_B_SHAR

- RTD_A_SHARE_DOME
- SNP

- E
- OTHER_RTD_SHARE
- NON_TRADABLE_SH
- NTRD_SHARE_STATE_
- NTRD_SHARE_STATE
- NTRD_SHARE_STATEJ
- NTRD_SHARE_DOME
- NTRD_SHARE_DOME
- NTRD_SHARE_IPOJUR
- NTRD_SHARE_GENJU
- NTRD_SHARE_STRA_I NVESTOR
- NTRD_SHARE_FUND
- NTRD_SHARE_NAT
- TRAN_SHARE
- FLOAT_SHARE_SENIO
- SHARE_INEMP
- PREFERRED_SHARE
- NTRD_SHARE_NLIST_
- STAQ_SHARE
- NET_SHARE
- SHARE_CHANGE_REA
- TOT_A_SHARE
- TOT_B_SHARE
- OTCA_SHARE
- OTCB_SHARE
- TOT_OTC_SHARE
- SHARE_HK
- PRE_NON_TRADABLE

- NTRD_SHARE_STRA_I
- NVESTOR

- _SHARE
- RESTRICTED_A_SHAR
- RTD_A_SHARE_STATE
- RTD_A_SHARE_STATE
- RTD_A_SHARE_OTHE
- RTD_A_SHARE_OTHE R_DOMESJUR
- TOT_RESTRICTED_SH

- RTD_A_SHARE_OTHE
- R_DOMESJUR

#### 3.5.6.4 股权冻结/质押

- 函数接口：get_equity_pledge_freeze
- 功能描述：获取指定股票列表的上市公司的股权冻结/质押数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 公告日期，本地数据缓存方案 |
| end_date | int | 否 | 公告日期，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| equity_pledge _freeze | dict | key：code value:dataframe column为equity_pledge_freeze的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
c t a e | base_data_object = ad.BaseData()
```

```python
| alendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
| ll_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
| quity_pledge_freeze = iinfo_data_object.get_equity_pledge_freeze (all_code_list)
```

- equity_pledge_freeze 的字段说明：

| 字段名称 | 类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | string | 证券代码 |  |
| ANN_DATE | string | 公告日期 |  |
| HOLDER_NAME | string | 股东名称 |  |
| HOLDER_TYPE_C | int | 股东类型代码ODE | 2:公司3:个人 |
| TOTAL_HOLDING | float_SHR" | 持股总数（万股） |  |
| TOTAL_HOLDING | float_SHR_RATIO | 持股总数占公司总股本比例 |  |
| FRO_SHARES | float数 | 本次冻结/质押股 |  |
| FRO_SHR_TO_TO | floatTAL_HOLDING_RATIO | 本次冻结/质押占 所持股比例 |  |
| FRO_SHR_TO_TO | floatTAL_RATIO | 本次冻结/质押占总股本比例 |  |
| TOTAL_PLEDGE_ | floatSHR | 累计冻结/质押股数 |  |
| IS_EQUITY_PLED | int | 是否股权质押回GE_REPO | 1:是0:否购 |
| BEGIN_DATE | string | 冻结/质押起始日 |  |
| END_DATE | string | 解冻/解押日期 |  |
| IS_DISFROZEN | int | 是否质押或解冻 | 1:是0:否 |
| FROZEN_INSTITU | stringTION | 执行冻结机构/质权方 |  |
| DISFROZEN_TIME | string | 解压或解冻日期 |  |
| SHR_CATEGORY_ | int | 股份性质类别代 | 1:法人股 2:个人股 3:国有 |

- float

- 本次冻结/质押占
- 所持股比例

| CODE | 码股6:流通股,限售流通股7:外资股 8:限售流通股 9:优先股 | 股4:国有股,法人股5:流通 |
| --- | --- | --- |
| FREEZE_TYPE | int | 冻结/质押类型购 |

#### 3.5.6.5 限售股解禁

- 函数接口：get_equity_restricted
- 功能描述：获取指定股票列表的上市公司的限售股解禁数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- equity_restrict ed

| i i t | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
| ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
| iinfo_data_object = ad.InfoData()
```

```python
| base_data_object = ad.BaseData()
```

```python
| calendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
| all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
| equity_restricted = iinfo_data_object.get_equity_restricted (all_code_list)
```

- equity_restricted 的字段说明：

| 字段名称 | 类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | string | 证券代码 |  |
| LIST_DATE | string | 解禁日期 |  |
| SHARE_RATIO | float | 解禁股占总股本比(%) |  |
| SHARE_LST_TYPE_NAME | string | 解禁股份类型名称 |  |
| SHARE_LST | int | 解禁数量（股） |  |
| SHARE_LST_IS_ANN | int | 上市数量是否公布值值 1: 是, 为实际公布值 | 0：否，为预测 |
| CLOSE_PRICE | float | 前日收盘价（元） |  |
| SHARE_LST_MARKET_VA | float | 解禁市值（元）LUE | SHARE_LST*CLOSE_PRICE |

股东权益数据
3.5.7

分红数据
3.5.7.1

- 函数接口：get_dividend
- 功能描述：获取指定股票列表的上市公司的分红数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 公告日期，本地数据缓存方案 |
| end_date | int | 否 | 公告日期，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| dividend | dataframe | column为dividend的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- today = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
dividend = iinfo_data_object.get_dividend(all_code_list)
```

dividend的字段说明：

| 字段名称 | 类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | string | 证券代码 |  |
| DIV_PROGRESS | DIV_PROGRESS | string | string |
| DVD_PER_SHARE_STK | float | 每股送转 |  |
| DVD_PER_SHARE_PRE_T | float | float | 每股派息(税前)(元)AX_CASH |
| DVD_PER_SHARE_AFTE | floatR_TAX_CASH | 每股派息(税后)(元) |  |
| DATE_EQY_RECORD | string | 股权登记日 |  |
| DATE_EX | string | 除权除息日 |  |
| DATE_DVD_PAYOUT | string | 派息日 |  |
| LISTINGDATE_OF_DVD_ | string | string | 红股上市日SHR |
| DIV_PRELANDATE | string | 预案公告日 | 董事会预案公告日期 |
| DIV_SMTGDATE | string | 股东大会公告日 |  |
| DATE_DVD_ANN | string | 分红实施公告日 |  |
| DIV_BASEDATE | string | 基准日期 |  |
| DIV_BASESHARE | float | 基准股本(万股) |  |
| CURRENCY_CODE | string | 货币代码 |  |
| ANN_DATE | string | 公告日期 |  |
| IS_CHANGED | IS_CHANGED | int | int |
| REPORT_PERIOD | string | 分红年度 |  |
| DIV_CHANGE | string | 方案变更说明 |  |
| DIV_BONUSRATE | float | 每股送股比例 |  |
| DIV_CONVERSEDRATE | float | 每股转增比例 |  |
| REMARK | string | 备注 |  |
| DIV_PREANN_DATE | string | 预案预披露公告日 | 股东提议的公告日期 |
| DIV_TARGET | string | 分红对象 |  |

配股数据
3.5.7.2

- 函数接口：get_right_issue
- 功能描述：获取指定股票列表的上市公司的配股数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 公告日期，本地数据缓存方案 |
| end_date | int | 否 | 公告日期，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| right_issue | dataframe | column为right_issue的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- today = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
right_issue = iinfo_data_object.get_right_issue(all_code_list)
```

right_issue的字段说明：

| 字段名称 | 类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | string | 证券代码 |  |
| PROGRESS | PROGRESS | int | int |
| PRICE | double | 配股价格(元) |  |
| RATIO | double | 配股比例 |  |
| AMT_PLAN | double | 配股计划数量(万股) |  |
| AMT_REAL | double | 配股实际数量(万股) |  |
| COLLECTION_FUND | double | 募集资金(元) |  |
| SHAREB_REG_DATE | string | 股权登记日 |  |
| EX_DIVIDEND_DATE | string | 除权日 |  |
| LISTED_DATE | string | 配股上市日 |  |

| PAY_START_DATE | string | 缴款起始日 |
| --- | --- | --- |
| PAY_END_DATE | string | 缴款终止日 |
| PREPLAN_DATE | string | 预案公告日 |
| SMTG_ANN_DATE | string | 股东大会公告日 |
| PASS_DATE | string | 发审委通过公告日 |
| APPROVED_DATE | string | 证监会核准公告日 |
| EXECUTE_DATE | string | 配股实施公告日 |
| RESULT_DATE | string | 配股结果公告日 |
| LIST_ANN_DATE | string | 上市公告日 |
| GUARANTOR | string | 基准年度 |
| GUARTYPE | double | 基准股本(万股) |
| RIGHTSISSUE_CODE | string | 配售代码 |
| ANN_DATE | string | 公告日期 |
| RIGHTSISSUE_YEAR | string | 配股年度 |
| RIGHTSISSUE_DESC | string | 配股说明 |
| RIGHTSISSUE_NAME | string | 配股简称 |
| RATIO_DENOMINATO | double | double |
| RATIO_MOLECULAR | double | 配股比例分子 |
| SUBS_METHOD | string | 认购方式 |
| EXPECTED_FUND_RA | double | double |

### 3.5.8 融资融券数据

#### 3.5.8.1 融资融券成交汇总

- 函数接口：get_margin_summary
- 功能描述：获取指定日期的上市公司的融资融券成交汇总数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 交易日，本地数据缓存方案 |
| end_date | int | 否 | 交易日，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| margin_summ ary | dataframe | column为margin_summary的字段 index为序号（无意义） |

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
margin_summary = iinfo_data_object.get_margin_summary()
```

margin_summary的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| TRADE_DATE | string | 交易日期 |
| SUM_BORROW_MONEY_BALANCE | float | 融资余额(元) |
| SUM_PURCH_WITH_BORROW_MONEY | float | 融资买入额(元) |
| SUM_REPAYMENT_OF_BORROW_MONE | floatY | 融资偿还额(元) |
| SUM_SEC_LENDING_BALANCE | float | 融券余额(元) |
| SUM_SALES_OF_BORROWED_SEC | int | 融券卖出量(股,份,手) |
| SUM_MARGIN_TRADE_BALANCE | float | 融资融券余额(元) |

#### 3.5.8.2 融资融券交易明细

- 函数接口：get_margin_detail
- 功能描述：获取指定股票列表的上市公司的融资融券交易明细数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 交易日，本地数据缓存方案 |
| end_date | int | 否 | 交易日，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| margin_detail | dict | key：code |

- value:dataframe column为margin_detail的字段 index为序号（无意义）

| i i t | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
margin_detail = iinfo_data_object.get_margin_detail(all_code_list)
```

margin_detail的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 证券代码 |
| SECURITY_NAME | string | 证券简称 |
| TRADE_DATE | string | 交易日期 |
| BORROW_MONEY_BALANCE" | float | 融资余额(元) |
| PURCH_WITH_BORROW_MON | floatEY | 融资买入额(元) |
| REPAYMENT_OF_BORROW_MO | floatNEY | 融资偿还额(元) |
| SEC_LENDING_BALANCE | float | 融券余额(元) |
| SALES_OF_BORROWED_SEC | int | 融券卖出量(股,份,手) |
| REPAYMENT_OF_BORROW_SE | intC | 融券偿还量(股,份,手) |
| SEC_LENDING_BALANCE_VOL | int | 融券余量(股,份,手) |
| MARGIN_TRADE_BALANCE | float | 融资融券余额(元) |

交易异动数据
3.5.9

龙虎榜
3.5.9.1

- 函数接口：get_long_hu_bang
- 功能描述：获取指定股票列表的上市公司的龙虎榜数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 交易日，本地数据缓存方案 |
| end_date | int | 否 | 交易日，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| long_hu_bang | dataframe | column为long_hu_bang的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- today = calendar[-1]

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
long_hu_bang = iinfo_data_object.get_long_hu_bang(all_code_list)
```

long_hu_bang的字段说明：

| 参数 | 数据类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| MARKET_CODE | string | 证券代码 |  |
| TRADE_DATE | string | 交易日期 |  |
| SECURITY_NAME | string | 证券名称 |  |
| REASON_TYPE | string型 | 上榜原因类 |  |

| REASON_TYPE_NAME | string | 上榜原因 |
| --- | --- | --- |
| CHANGE_RANGE | float | 涨跌幅（%） |
| TRADER_NAME | string | 营业部名称 |
| BUY_AMOUNT | float（元） | 买 入 金 额 |
| SELL_AMOUNT | float（元） | 卖 出 金 额 |
| FLOW_MARK | int | 买卖表示 |
| TOTAL_AMOUNT | float额（元） | 实际交易金 |
| TOTAL_VOLUME | float（万股） | 实际交易量 |

大宗交易
3.5.9.2

- 函数接口：get_block_trading
- 功能描述：获取指定股票列表的大宗交易数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深A的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 交易日，本地数据缓存方案 |
| end_date | int | 否 | 交易日，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| block_trading | dataframe | column为block_trading的字段 index为序号（无意义） |

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

| t | oday = calendar[-1] |

```python
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

- end_date=today)

```python
block_trading = iinfo_data_object. block_trading (all_code_list)
```

block_trading的字段说明：

| 参数 | 数据类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 证券代码 |
| TRADE_DATE | string | 交易日期 |
| B_SHARE_PRICE | float | 成交价（元） |
| B_SHARE_VOLUME | float | 成交量（万股） |
| B_FREQUENCY | int | 笔数 |
| BLOCK_AVG_VOLUME | float | 每笔成交数量（万股份） |
| B_SHARE_AMOUNT | float | 成交金额（万元） |
| B_BUYER_NAME | string | 买方营业部名称 |
| B_SELLER_NAME | string | 卖方营业部名称 |

期权数据
3.5.10

期权基本资料
3.5.10.1

函数接口：get_option_basic_info

- 功能描述：获取指定期权的基本资料（沪深交易所的ETF期权）
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深ETF期权的的代码列表，可见示 例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| option_basic_ info | dataframe | column为option_basic_info的字段 index为序号（无意义） |

| i i t | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- oday = calendar[-1]

```python
code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
```

- hist_code_list =

```python
base_data_object.get_hist_code_list(security_type='EXTRA_ETF_OP'', start_date=20130101,
```

- end_date=today)

```python
option_basic_info =iinfo_data_object.get_option_basic_info(code_list, is_local=False)
```

option_basic_info的字段说明：

| 参数 | 数据类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| CONTRACT_FULL_NAME | string | 合约全称 |  |
| CONTRACT_TYPE | string | 合约类别P表示认沽 | C表示认购 |
| DELIVERY_MONTH | string | 交割月份 |  |
| EXPIRY_DATE | string | 到期日 |  |
| EXERCISE_PRICE | float | 行权价格 |  |
| EXERCISE_END_DATE | string | 最后行权日 |  |
| START_TRADE_DATE | string | 开始交易日 |  |
| LISTING_REF_PRICE | float | 挂牌基准价 |  |
| LAST_TRADE_DATE | string | 最后交易日 |  |
| EXCHANGE_CODE | string | 合约交易所代码 |  |
| DELIVERY_DATE | string | 最后交割日 |  |
| CONTRACT_UNIT | Int | 合约单位 |  |
| IS_TRADE | string | 是否交易 |  |
| EXCHANGE_SHORT_NAME | string | 合约交易所简称 |  |
| CONTRACT_ADJUST_FLAG | string | 合约调整标志 |  |
| MARKET_CODE | string | 合约代码 |  |

期权标准合约属性
3.5.10.2

函数接口：get_option_std_ctr_specs

功能描述：获取指定期权标准合约属性（沪深交易所的 ETF期权）

输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深 ETF 的的代码列表，目前包含 159919.SZ 159915.SZ 159922.SZ 159901.SZ 510300.SH 588000.SH 588080.SH 510050.SH 510500.SH |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| option_std_ctr _specs | dataframe | column为option_std_ctr_specs的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
option_std_ctr_specs =iinfo_data_object.get_option_std_ctr_specs(['510050.SH'], is_local=False)
```

option_std_ctr_specs的字段说明：

| 参数 | 数据类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| EXERCISE_DATE | string | 期权行权日 |  |
| CONTRACT_UNIT | int | 合约单位 |  |
| POSITION_DECLARE_MIN | string | 头寸申报下限 |  |
| QUOTE_CURRENCY_UNIT | string | 报价货币单位 |  |
| LAST_TRADING_DATE | string | 最后交易日 |  |

| POSITION_LIMIT | string | 头寸限制 |
| --- | --- | --- |
| DELIST_DATE | string | 退市日期 |
| NOTIONAL_VALUE | string | 立约价值 |
| EXERCISE_METHOD | string | 行权方式 |
| DELIVERY_METHOD | string | 交割方式 |
| SETTLEMENT_MONTH | string | 合约结算月份 |
| TRADING_FEE | string | 交易费用 |
| EXCHANGE_NAME | string | 交易所名称 |
| OPTION_EN_NAME | string | 期权英文名称 |
| CONTRACT_VALUE | float | 合约价值 |
| IS_SIMULATION | int | 是否仿真合约 |
| CONTRACT_UNIT_DIMENSI | stringON | 合约单位量纲 |
| OPTION_STRIKE_PRICE | string | 期权行权价 |
| IS_SIMULATION_TRADE | string0 否 1 是 | 是否仿真交易 |
| LISTED_DATE | string | 上市日期 |
| OPTION_NAME | string | 期权名称 |
| PREMIUM | string | 期权金 |
| OPTION_TYPE | string | 期权类型 |
| TRADING_HOURS_DESC | string | 交易时间说明 |
| FINAL_SETTLEMENT_DATE | string | 最后结算日 |
| FINAL_SETTLEMENT_PRICE | string | 最后结算价 |
| MIN_PRICE_UNIT | string | 最小报价单位 |
| MARKET_CODE | string | 市场代码 |
| CONTRACT_MULTIPLIER | int | 合约乘数 |

期权月合约属性变动
3.5.10.3

函数接口：get_option_mon_ctr_specs

- 功能描述：获取指定期权月合约属性变动（沪深交易所的ETF期权）
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深ETF期权的的代码列表，可见示 例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| block_trading | dataframe | column为block_trading的字段 index为序号（无意义） |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
calendar = base_data_object.get_calendar()
```

- today = calendar[-1]

```python
code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
```

- hist_code_list =

```python
base_data_object.get_hist_code_list(security_type='EXTRA_ETF_OP'', start_date=20130101,
```

- end_date=today)

```python
option_mon_ctr_specs =iinfo_data_object.get_option_mon_ctr_specs(code_list, is_local=False)
```

option_mon_ctr_specs的字段说明：

| 参数 | 数据类型 | 字段说明 |
| --- | --- | --- |
| CODE_OLD | string | 原交易代码 |
| CHANGE_DATE | string | 调整日期 |
| MARKET_CODE | string | 市场代码 |
| NAME_NEW | string | 新合约简称 |
| EXERCISE_PRICE_NEW | float | 新行权价(元) |
| NAME_OLD | string | 原合约简称 |
| CODE_NEW | string | 新交易代码 |
| EXERCISE_PRICE_OLD | float | 原行权价(元) |
| UNIT_OLD | float | 原合约单位(股) |

| UNIT_NEW | float | 新合约单位(股) |
| --- | --- | --- |
| CHANGE_REASON | string | 调整原因 |

### 3.5.11 ETF 数据

每日最新申赎数据
3.5.11.1 ETF

函数接口： get_etf_pcf

- 功能描述：获取指定ETF的申赎和成分股数据（沪深交易所的ETF）
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持沪深ETF的的代码列表，可见示例 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| etf_pcf_info | dataframe | column为etf_pcf_info的字段 index为ETF代码 |
| etf_pcf_consti tuent | dict | 字典的key：ETF代码 字典的value：dataframe， column为etf_pcf_constituent的字段， index为序号 |

| i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_hist_code_list(security_type='EXTRA_ETF')
```

```python
etf_pcf_info, etf_pcf_constituent = base_data_object.get_etf_pcf(code_list)
```

etf_pcf_info的字段说明：

| 参数 | 数据类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| creation_redemption_unit | intETF份数 | 每个篮子对应的 |  |
| max_cash_ratio | string例 | 最大现金替代比 |  |
| publish | string | 是否发布IOPV | Y=是,N=否 |
| creation | string | 是否允许申购有效) | Y=是,N=否(仅深圳 |
| redemption | string | 是否允许赎回 | Y=是,N=否(仅深圳 |

- 有效)
- creation_redemption_switch
- record_num
- total_record_num
- estimate_cash_component
- trading_day
- pre_trading_day
- cash_component
- nav_per_cu
- nav
- symbol
- fund_management_company
- underlying_security_id
- underlying_security_id_source
- dividend_per_cu
- creation_limit
- redemption_limit
- creation_limit_per_user
- redemption_limit_per_user

| net_creation_limit | int | 净申购总额限制深圳有效) | 0 表示没有限制(仅 |
| --- | --- | --- | --- |
| net_redemption_limit | int | 净赎回总额限制深圳有效) | 0 表示没有限制(仅 |
| net_creation_limit_per_user | int | 单个账户净申购总额限制 | 0 表示没有限制(仅深圳有效) |
| net_redemption_limit_per_user | int | 单个账户净赎回总额限制 | 0 表示没有限制(仅深圳有效) |

etf_pcf_constituent的字段说明：

| 参数 | 数据类型 | 字段说明 | 备注 |
| --- | --- | --- | --- |
| underlying_symbol | string | 成份证券简称 |  |
| component_share | int | 成份证券数量 |  |
| substitute_flag | string | 现金替代标志标志* //0=禁止现金替代(必须有证券),1=可以进行现金替代(先用证券,证券不足时差额部分用现金替代),2=必须用现金替代//**上海现金替代标志*//ETF 公 告 文 件1.0 版格式//0 –沪市不可被替代, 1 – 沪市可以被替代, 2 – 沪市必须被替代, 3 – 深市退补现金替代, 4 – 深市必须现金替代//5 – 非沪深市场成份证券退补现金替代(不适用于跨沪 深 港 ETF 产品), 6 – 非沪深市场成份证券必须现 | //**深圳现金替代 |

- 金替代(不适用于
- 跨沪深港 ETF 产
- 品)
- //ETF 公 告 文 件
- 2.1 版格式
- //0 –沪市不可被替
- 代, 1 – 沪市可以被
- 替代, 2 – 沪市必须
- 被替代, 3 – 深市退
- 补现金替代, 4 – 深
- 市必须现金替代
- //5 – 非沪深市场
- 成份证券退补现金
- 替代(不适用于跨
- 沪 深 港 ETF 产
- 品), 6 – 非沪深市
- 场成份证券必须现
- 金替代(不适用于
- 跨沪深港 ETF 产
- 品)
- //7 – 港市退补现
- 金替代(仅适用于
- 跨沪深港 ETF 产
- 品),
- //8 – 港市必须现
- 金替代(仅适用于
- 跨沪深港 ETF 产
- 品)
- premium_ratio
- discount_ratio
- creation_cash_substitute
- redemption_cash_substitute
- substitution_cash_amount
- underlying_security_id

基金份额
3.5.11.2 ETF

- 函数接口：get_fund_share
- 功能描述：获取指定ETF列表的基金份额数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- fund_share

| i i f | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
etf_code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
```

- # ETF份额

```python
und_share = iinfo_data_object.get_fund_share(etf_code_list, is_local=False)
```

- fund_share 的字段说明：
- 字段名称
- FUND_SHARE
- CHANGE_REASON
- IS_CONSOLIDATED_DATA

| MARKET_CODE | string | 市场代码 |
| --- | --- | --- |
| ANN_DATE | string | 公告日期 |
| TOTAL_SHARE | float | 基金总份额(万份) |
| CHANGE_DATE | string | 变动日期 |
| FLOAT_SHARE | float | 流通份额(万份) |

每日收盘
3.5.11.3 ETF iopv

- 函数接口：get_fund_iopv
- 功能描述：获取指定ETF列表的基金份额数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- fund_iopv

| i i f | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
etf_code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
```

- # ETF份额

```python
und_iopv = iinfo_data_object.get_fund_iopv(etf_code_list, is_local=False)
```

- fund_iopv 的字段说明：

| 字段名称 | 类型 | 字段说明 |

| MARKET_CODE | string | 市场代码 |
| --- | --- | --- |
| PRICE_DATE | string | 日期 |
| IOPV_NAV | float | IOPV收盘净值 |

交易所指数数据
3.5.12

交易所指数成分股
3.5.12.1

- 函数接口：get_index_constituent
- 功能描述：获取指定交易所指数列表的成分股数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- index_constit uent

| i i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list(security_type='EXTRA_INDEX_A')
```

```python
ndex_constituent = iinfo_data_object.get_index_constituent(code_list, is_local=False)
```

- index_constituent 的字段说明：
- 字段名称
- INDEX_CODE
- CON_CODE
- INDATE
- OUTDATE
- INDEX_NAME

#### 3.5.12.2 交易所指数成分股日权重

- 函数接口：get_index_weight
- 功能描述：获取指定交易所指数列表的成分股日权重数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- index_weight

| # i | 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
a i i i | d.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
ndex_weight = iinfo_data_object.get_index_weight(['000016.SH', '000300.SH', '000905.SH','000906.SH','000852.SH'],
```

- s_local=False)

index_weight 的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| INDEX_CODE | string | 指数代码 |
| CON_CODE | string | 标的代码 |
| TRADE_DATE | string | 生效日期 |
| TOTAL_SHARE | float | 总股本（股） |
| FREE_SHARE_RATIO | float档后） | 自由流通比例（%）（归 |
| CALC_SHARE | float | 计算用股本（股） |
| WEIGHT_FACTOR | float | 权重因子 |
| WEIGHT | float | 权重（%） |
| CLOSE | float | 收盘价 |

行业指数数据
3.5.13

#### 3.5.13.1 行业指数基本信息

- 函数接口：get_industry_base_info
- 功能描述：获取行业指数的基本信息数据
- 输入参数：
- 参数
- local_path
- is_local

- 输出参数：
- 参数
- industry_base _info

| i a i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
d.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
ndustry_base_info = iinfo_data_object.get_industry_base_info()
```

- industry_base_info 的字段说明：
- 字段名称
- INDEX_CODE
- INDUSTRY_CODE
- LEVEL_TYPE
- LEVEL1_NAME
- LEVEL2_NAME
- LEVEL3_NAME
- IS_PUB
- CHANGE_REASON

#### 3.5.13.2 行业指数成分股

- 函数接口：get_industry_constituent
- 功能描述：获取指定行业指数列表的成分股数据
- 输入参数：
- 参数
- code_list

| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ 'D://AmazingData_local_data//' ” |
| --- | --- | --- | --- |
| is_local | bool | 否 | 默认为True，仅从本地获取，不从服务器 获取数据； False ，仅从服务器获取，不从本地获取 数据； 因为原始数据的剔除日期会根据最新数 据修改，所以第一次运行is_local 需要设 置成 False 才会从服务器获取数据。 |

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| industry_cons tituent | dict | key：code value:dataframe column为industry_constituent的字段 index为日期 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
industry_base_info = iinfo_data_object.get_industry_base_info()
```

- industry_base_list = list(industry_base_info['INDEX_CODE'])
- # 行业指数成分股

```python
industry_constituent = iinfo_data_object.get_industry_constituent(industry_base_list, is_local=False)
```

- industry_constituent 的字段说明：
- 字段名称
- INDEX_CODE
- CON_CODE
- INDATE
- OUTDATE
- INDEX_NAME

行业指数成分股日权重
3.5.13.3

- 函数接口：get_industry_weight
- 功能描述：获取指定行业指数列表的成分股日权重数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- industry_weig ht

| i i i i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
ndustry_base_info = iinfo_data_object.get_industry_base_info()
```

- ndustry_base_list = list(industry_base_info['INDEX_CODE'])
- # 行业指数日权重

```python
ndustry_weight = iinfo_data_object.get_industry_weight(industry_base_list)
```

的字段说明：
industry_weight

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| WEIGHT | float | 权重 |
| CON_CODE | string | 成份股代码 |
| TRADE_DATE | string | 交易日期 |
| INDEX_CODE | string | 指数代码 |

行业指数日行情
3.5.13.4

- 函数接口：get_industry_daily
- 功能描述：获取指定行业指数列表的日行情数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local
- begin_date
- end_date

- 输出参数：
- 参数
- industry_daily

| i i i i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
ndustry_base_info = iinfo_data_object.get_industry_base_info()
```

- ndustry_base_list = list(industry_base_info['INDEX_CODE'])
- # 行业指数日行情

```python
ndustry_daily = iinfo_data_object.get_industry_daily(industry_base_list, is_local=False)
```

的字段说明：
industry_daily

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| OPEN | float | 开盘价 |
| HIGH | float | 最高价 |
| CLOSE | float | 收盘价 |
| LOW | float | 最低价 |
| AMOUNT | float | 成交金额(元) |

| VOLUME | float | 成交量(股) |
| --- | --- | --- |
| PB | float | 指数市净率 |
| PE | float | 指数市盈率 |
| TOTAL_CAP | float | 总市值(万元) |
| A_FLOAT_CAP | float | A股流通市值(万元) |
| INDEX_CODE | string | 指数代码 |
| PRE_CLOSE | float | 昨收盘价 |
| TRADE_DATE | string | 交易日期 |

### 3.5.14 可转债数据

#### 3.5.14.1 可转债发行

- 函数接口：get_kzz_issuance
- 功能描述：获取指定可转债列表的可转债发行数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_issuance

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_issuance = iinfo_data_object.get_kzz_issuance(code_list, is_local=False)
```

的字段说明：
kzz_issuance

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| STOCK_CODE | string | 正股代码 |
| CRNCY_CODE | string | 货币代码 |
| ANN_DT | string | 公告日期 |
| PRE_PLAN_DATE | string | 预案公告日 |
| SMTG_ANN_DATE | string | 股东大会公告日 |
| LISTED_ANN_DATE | string | 上市公告日 |
| LISTED_DATE | string | 上市日期 |
| PLAN_SCHEDULE | string1: 董事会预案2: 股东大会通过3: 实施4: 未通过5: 证监会通过6: 达成转让意向7: 签署转让协议8: 国资委批准9: 商务部批准10: 过户11: 延期实施12: 停止实施13: 分红方案待定 | 方案进度 |
| IS_SEPARATION | int | 是否分离交易可转债 |
| RECOMMENDER | string | 上市推荐人 |
| CLAUSE_IS_INT_CHA_DE | intPO_RATE | 利率是否随存款利率调整 |
| CLAUSE_IS_COM_INT | int | 是否有利息补偿条款 |
| CLAUSE_COM_INT_RATE | float | 补偿利率（%） |
| CLAUSE_COM_INT_DESC | string | 补偿利率说明 |
| CLAUSE_INIT_CONV_PRI | stringCE_ITEM | 初始转股价条款 |
| CLAUSE_CONV_ADJ_ITE | stringM | 转股价格调整条款 |

| CLAUSE_CONV_PERIOD_I | stringTEM | 转换期条款 |
| --- | --- | --- |
| CLAUSE_INI_CONV_PRIC | floatE | 初始转换价格 |
| CLAUSE_INI_CONV_PRE | floatMIUM_RATIO | 初始转股价溢价比例（%） |
| CLAUSE_PUT_ITEM | string | 回售条款 |
| CLAUSE_CALL_ITEM | string | 赎回条款 |
| CLAUSE_SPEC_DOWN_A | stringDJ | 特别向下修正条款 |
| CLAUSE_ORIG_RATION_A | stringRR_ITEM | 向原股东配售安排条款 |
| LIST_PASS_DATE | string | 发审通过公告日 |
| LIST_PERMIT_DATE | string | 证监会核准公告日 |
| LIST_ANN_DATE | string | 发行公告日 |
| LIST_RESULT_ANN_DATE | string | 发行结果公告日 |
| LIST_TYPE | string | 发行方式 |
| LIST_FEE | float | 发行费用 |
| LIST_RATION_DATE | string | 老股东配售日期 |
| LIST_RATION_REG_DATE | string | 老股东配售股权登记日 |
| LIST_RATION_PAYMT_DA | stringTE | 老股东配售缴款日 |
| LIST_RATION_CODE | string | 老股东配售代码 |
| LIST_RATION_NAME | string | 老股东配售简称 |
| LIST_RATION_PRICE | float | 老股东配售价格 |
| LIST_RATION_RATIO_DE | float | 老股东配售比例分母 |
| LIST_RATION_RATIO_MO | float | 老股东配售比例分子 |
| LIST_RATION_VOL | float(张)） | 向 老 股 东 配 售 数 量 |
| LIST_HOUSEHOLD | float | 老股东配售户数 |
| LIST_ONL_DATE | string | 上网发行日期 |

| LIST_PCHASE_CODE_ONL | string | 上网发行申购代码 |
| --- | --- | --- |
| LIST_PCH_NAME_ONL | string | 上网发行申购名称 |
| LIST_PCH_PRICE_ONL | float | 上网发行申购价格 |
| LIST_ISSUE_VOL_ONL | float先配售)(张) | 上网发行数量(不含优 |
| LIST_CODE_ONL | float | 上网发行配号总数 |
| LIST_EXCESS_PCH_ONL | float(不含优先配售) | 上网发行超额认购倍数 |
| RESULT_EF_SUBSCR_P_O | floatFF | 网上有效申购户数(不含优先配售) |
| RESULT_SUC_RATE_OFF | float含优先配售) | 网上有效申购手数(不 |
| LIST_DATE_INST_OFF | string日期 | 网下向机构投资者发行 |
| LIST_VOL_INST_OFF | float数量(不含优先配售)(张) | 网下向机构投资者发行 |
| RESULT_SUC_RATE_ON | float配售)(%) | 网上中签率(不含优先 |
| LIST_EFFECT_PC_HVOL_ | floatOFF | 网下有效申购手数(不含优先配售) |
| LIST_EFF_PC_H_OF | float含优先配售) | 网下有效申购户数(不 |
| LIST_SUC_RATE_OFF | float配售)(%) | 网下中签率(不含优先 |
| PRE_RATION_VOL | float | 网下优先配售数量(张) |
| LIST_ISSUE_SIZE | float | 发行规模(万元) |
| LIST_ISSUE_QUANTITY | float | 发行数量(万张) |
| MIN_OFF_INST_SUBSCR_ | floatQTY | 网下最小申购数量(机构) |
| OFF_INST_DEP_RATIO | string | 网下定金比例(机构) |
| MAX_OFF_INST_SUBSCR_ | floatQTY | 网下最大申购数量(机构) |

| OFF_SUBSCR_UNIT_INC_ | stringDESC | 网下申购累进单位说明 |
| --- | --- | --- |
| IS_CONV_BONDS | int | 是否可转债 |
| MIN_UNLINE_PUBLIC | float众)(元) | 网下最小申购数量(公 |
| MAX_UNLINE_PUBLIC | float众)(元) | 网上最大申购数量(公 |
| TERM_YEAR | float | 借款期限(年) |
| INTEREST_TYPE | string | 利率类型 |
| COUPON_RATE | float | 利率(%) |
| INTEREST_FRE_QUENCY | string | 付息频率 |
| RESULT_SUC_RATE_ON2 | float配售)(%) | 网上中签率(不含优先 |
| COUPON_TXT | string | 利率说明 |
| RATIO_ANNCE_DATE | string | 网上中签率公告日 |
| RATIO_DATE | string | 网上中签结果公告日 |

#### 3.5.14.2 可转债份额

- 函数接口：get_kzz_share
- 功能描述：获取指定可转债列表的可转债份额数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_share

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_share = iinfo_data_object.get_kzz_share(code_list, is_local=False)
```

的字段说明：
kzz_share

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| CHANGE_DATE | string | 变动日期 |
| ANN_DATE | string | 公告日期 |
| MARKET_CODE | string | 市场代码 |
| BOND_SHARE | float | 债券份额（万元） |
| CONV_SHARE | float | 已转成股份数 |
| CHANGE_REASON | string的枚举类型:ZZGSHKZZSHSDQQLXQTQDF付GHHSZGHGZG | 变动原因代码，目前包含转债转股赎回可转债上市回售到期权利行权本金提前兑购回回售转股回购转股 |

#### 3.5.14.3 可转债转股数据

- 函数接口：get_kzz_conv
- 功能描述：获取指定可转债列表的可转债转股数据
- 输入参数：
- 参数
- code_list
- local_path

- 类似“ 'D://AmazingData_local_data//' ”
- is_local

- 输出参数：
- 参数
- kzz_conv

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_conv = iinfo_data_object.get_kzz_conv(code_list, is_local=False)
```

kzz_conv 的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| ANN_DATE | string | 公告日期 |
| CONV_CODE | string | 转股申报代码 |
| CONV_NAME | string | 转股简称 |
| CONV_PRICE | float | 股转价格 |
| CURRENCY_CODE | string | 股转申报代码 |
| CONV_START_DATE | string | 自愿转换期起始日 |
| CONV_END_DATE | string | 自愿转换期截止日 |
| TRADE_DATE_LAST | string | 可转换债停止交易日 |
| FORCED_CONV_DATE | string | 强制转换日 |
| FORCED_CONV_PRICE | float | 强制转换价格 |
| REL_CONV_MONTH | float | 相对转换期(月) |
| IS_FORCED | float | 是否强制转股 |
| FORCED_CONV_REASON | string | 强制转换原因 |

可转债转股变动数据
3.5.14.4

- 函数接口：get_kzz_conv_change
- 功能描述：获取指定可转债列表的可转债转股变动数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_conv_cha nge

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_conv_change = iinfo_data_object.get_kzz_conv_change(code_list, is_local=False)
```

kzz_conv_change 的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| CHANGE_DATE | string | 变动日期 |
| ANN_DATE | string | 公告日期 |
| CONV_PRICE | float | 转股价格 |
| CHANGE_REASON | string变动原因12 | 变动原因，变动原因名称发行换股吸收合并 |

| 3 | 派息 |
| --- | --- |
| 4 | 配股 |
| 5 | 上市 |
| 6 | 送股 |
| 7 | 送转股 |
| 8 | 送转股,派息 |
| 9 | 修正 |
| 10 | 增发 |
| 11 | 转增,派息 |
| 12 | 送股,派息 |
| 13 | 公司选择不行 |
| 使赎回权 |  |
| 14 | 回购注销 |
| 15 | 回购注销,派息 |
| 16 | 增发,回购注销 |
| 17 | 增发,回购注销, |
| 派息 |  |
| 18 | 增发,派息 |
| 19 | 换股 |
| 20 | 派息,转增 |
| 21 | 派息,转增,增发 |
| 22 | 派息,送转股 |
| 24 | 调整 |
| 25 | 转增 |
| 26 | 除息 |

#### 3.5.14.5 可转债修正数据

- 函数接口：get_kzz_corr
- 功能描述：获取指定可转债列表的可转债修正数据
- 输入参数：

| 参数 | 数据类型 | 必选 | 解释 |
| --- | --- | --- | --- |
| code_list | list[str] | 是 | 支持可转债的的代码列表 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式 类似“ |

- 'D://AmazingData_local_data//' ”
- is_local

- 输出参数：
- 参数
- kzz_corr

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_corr = iinfo_data_object.get_kzz_corr(code_list, is_local=False)
```

kzz_corr 的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| START_DATE | string | 特别修正起始时间 |
| END_DATE | string | 特别修正结束时间 |
| CORR_TRIG_CALC_MAX_ | floatPERIOD | 修正触发计算最大时间区间（天） |
| CORR_TRIG_CALC_PERIO | floatD | 修正触发计算时间区间（天） |
| SPEC_CORR_TRIG_RATIO | float | 特别修正触发比例（% |
| CORR_CONV_PRICE_FLO | stringOR_DESC | 修正后转股价格底线说明 |
| REF_PRICE_IS_AVG_PRIC | intE | 参考价格是否为算术平均价 |
| CORR_TIMES_LIMIT | string | 修正次数限制 |
| IS_TIMEPOINT_CORR_CL | intAUSE_FLAG | 是否有时点修正条款 |
| TIMEPOINT_COUNT | float | 时点数 |
| TIMEPOINT_CORR_TEXT_ | stringCLAUSE | 时点修正文字条款 |

| SPEC_CORR_RANGE | float | 特别修正幅度 |
| --- | --- | --- |
| IS_SPEC_DOWN_CORR_C | intLAUSE_FLAG | 是否有特别向下修正条款 |

可转债赎回数据
3.5.14.6

- 函数接口：get_kzz_call
- 功能描述：获取指定可转债列表的可转债赎回数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_call

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_call = iinfo_data_object.get_kzz_call(code_list, is_local=False)
```

kzz_call 的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| CALL_PRICE | float | 赎回价 |
| BEGIN_DATE | string | 起始日期 |
| END_DATE | string | 截止日期 |
| TRI_RATIO | float | 触发比例（%） |

可转债回售数据
3.5.14.7

- 函数接口：get_kzz_put
- 功能描述：获取指定可转债列表的可转债回售数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_put

| i a i c | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
d.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
ode_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_put = iinfo_data_object.get_kzz_put(code_list, is_local=False)
```

kzz_put 的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| PUT_PRICE | float | 回售价 |
| BEGIN_DATE | string | 起始日期 |
| END_DATE | string | 截止日期 |
| TRI_RATIO | float | 触发比例（%） |

可转债回售赎回条款
3.5.14.8

- 函数接口：get_kzz_put_call_item
- 功能描述：获取指定可转债列表的可转债回售赎回条款数据

- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_put_call_ item

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_put_call_item = iinfo_data_object.get_kzz_put_call_item(code_list, is_local=False)
```

的字段说明：
kzz_put_call_item

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| MAND_PUT_PERIOD | string | 无条件回售期 |
| MAND_PUT_PRICE | float | 无条件回售价 |
| MAND_PUT_START_DATE | string | 无条件回售开始日期 |
| MAND_PUT_END_DATE | string | 无条件回售结束日期 |
| MAND_PUT_TEXT | string | 无条件回售文字条款 |
| IS_MAND_PUT_CONTAIN | int_CURRENT | 无条件回售是否含当期利息 |
| CON_PUT_START_DATE | string | 有条件回售起始日期 |
| CON_PUT_END_DATE | string | 有条件回售结束日期 |
| MAX_PUT_TRI_PER | float区间 | 回售触发计算最大时间 |
| PUT_TRI_PERIOD | float | 回售触发计算时间区间 |

| ADD_PUT_CON | string | 附加回售条件 |
| --- | --- | --- |
| ADD_PUT_PRICE_INS | string | 股价回售价格说明 |
| PUT_NUM_INS | string | 回售次数说明 |
| PUT_PRO_PERIOD | float | 相对回售期（月） |
| PUT_NO_PERY | float | 每年回售次数 |
| IS_PUT_ITEM | int | 是否有回售条款 |
| IS_TERM_PUT_ITEM | int | 是否有到期回售条款 |
| IS_MAND_PUT_ITEM | int | 是否有无条件回售条款 |
| IS_TIME_PUT_ITEM | int | 是否有时点回售条款 |
| TIME_PUT_NO | float | 时点回售数 |
| TIME_PUT_ITEM | string | 时点回售文字条款 |
| TERM_PUT_PRICE | float | 到期回售价 |
| CON_CALL_START_DATE | string | 有条件赎回起始日期 |
| CON_CALL_END_DATE | string | 有条件赎回结束日期 |
| CALL_TRI_CON_INS | string | 赎回触发条件说明 |
| MAX_CALL_TRI_PER | float区间 | 赎回触发计算最大时间 |
| CALL_TRI_PER | float | 赎回触发计算时间区间 |
| CALL_NUM_BER_INS | string | 赎回次数说明 |
| IS_CALL_ITEM | int | 是否有赎回条款 |
| CALL_PRO_PERIOD | float | 相对赎回期（月） |
| CALL_NO_PERY | float | 每年赎回次数 |
| IS_TIME_CALL_ITEM | int | 是否有时点赎回条款 |
| TIME_CALL_NO | float | 时点赎回数 |
| TIME_CALL_TEXT | string | 时点赎回文字条款 |
| EXPIRED_REDEMPTION_P | floatRICE | 到期赎回价 |
| PUT_TRI_CON_DESC | string | 回售触发条件说明 |

可转债回售条款执行说明
3.5.14.9

- 函数接口：get_kzz_put_explanation
- 功能描述：获取指定可转债列表的可转债回售条款执行说明数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_put_expla nation

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_put_explanation = iinfo_data_object.get_kzz_put_explanation(code_list, is_local=False)
```

kzz_put_explanation 的字段说明：

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| PUT_FUND_ARRIVAL_DA | stringTE | 回售资金到账日 |
| PUT_PRICE | float（元） | 每百元面值回收价格 |
| PUT_ANNOUNCEMENT_D | stringATE | 回售公告日 |
| PUT_EX_DATE | string | 回售履行结果公告日 |
| PUT_AMOUNT | float | 回售总面额（亿元） |
| PUT_OUTSTANDING | float | 继续托管总面额（亿元 |

| REPURCHASE_START_DA | stringTE | 回售行使开始日 |
| --- | --- | --- |
| REPURCHASE_END_DATE | string | 回售行使截止日 |
| RESALE_START_DATE | string | 转售开始日 |
| FUND_END_DATE | string | 回售日 |
| REPURCHASE_CODE | string | 回售代码 |
| RESALE_AMOUNT | float | 转售总面额（亿元） |
| RESALE_IMP_AMOUNT | float | 实施转售总面额（亿元 |
| RESALE_END_DATE | string | 转售截止日 |

#### 3.5.14.10 可转债赎回条款执行说明

- 函数接口：get_kzz_call_explanation
- 功能描述：获取指定可转债列表的可转债赎回条款执行说明数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_call_expl anation

| i i | # 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
base_data_object = ad.BaseData()
```

```python
code_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
kzz_call_explanation = iinfo_data_object.get_kzz_call_explanation(code_list, is_local=False)
```

的字段说明：
kzz_call_explanation

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| CALL_DATE | string | 赎回日 |
| CALL_PRICE | float | 每百元面值赎回价格(元) |
| CALL_ANNOUNCEMENT_DATE | string | 赎回公告日 |
| CALL_FUL_RES_ANN_DATE | string | 赎回履行结果公告日 |
| CALL_AMOUNT | float | 赎回总面额(亿元) |
| CALL_OUTSTANDING_AMOUNT | float | 继续托管总面额（亿元） |
| CALL_DATE_PUB | string | 赎回日（公布） |
| CALL_FUND_ARRIVAL_DATE | string | 赎回资金到账日 |
| CALL_RECORD_DAY | string | 赎回登记日 |
| CALL_REASON | string | 赎回原因 |

#### 3.5.14.11 可转债停复牌信息

- 函数接口：get_kzz_suspend
- 功能描述：获取指定可转债列表的可转债停复牌信息数据
- 输入参数：
- 参数
- code_list
- local_path
- is_local

- 输出参数：
- 参数
- kzz_suspend

| # i a i | 第一步 登录api |
| --- | --- |
| mport AmazingData as ad |  |

```python
d.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
info_data_object = ad.InfoData()
```

```python
b c k | ase_data_object = ad.BaseData()
```

```python
ode_list = base_data_object.get_code_list('EXTRA_KZZ')
```

```python
zz_suspend = iinfo_data_object.get_kzz_suspend(code_list, is_local=False)
```

的字段说明：
kzz_suspend

| 字段名称 | 类型 | 字段说明 |
| --- | --- | --- |
| MARKET_CODE | string | 市场代码 |
| SUSPEND_DATE | string | 停牌日期 |
| SUSPEND_TYPE | int001-上午停牌002-下午停牌003-今起停牌004-盘中停牌007-停牌1小时016-停牌1天 | 停牌类型代码 |
| RESUMP_DATE | string | 复牌日期 |
| CHANGE_REASON | string | 停牌原因 |
| CHANGE_REASON_CODE | int | 停牌原因代码 |
| RESUMP_TIME | string | 停复牌时间 |

国债收益率数据
3.5.15

国债收益率
3.5.15.1

- 函数接口：get_treasury_yield
- 功能描述：获取指定期限的国债收益率数据
- 输入参数：
- 参数
- term_list

- 'y30'：30年
- local_path
- is_local
- begin_date
- end_date

输出参数：

| 参数 | 数据类型 | 解释 |
| --- | --- | --- |
| treasury_yield | dict | 字典的key：期限 字典的value：dataframe， column为YIELD，国债收益率数据， index为日期 |

- # 第一步 登录api

```python
import AmazingData as ad
```

```python
ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

```python
iinfo_data_object = ad.InfoData()
```

```python
treasury_yield = iinfo_data_object.get_treasury_yield(['m3', 'm6', 'y1', 'y2', 'y3', 'y5', 'y7', 'y10', 'y30'])
```

4. 附录

## 4.1 字段取值说明

### 4.1.1 代码类型 security_type(沪深北)

| 数据类型 | 枚举值 | 说明 |
| --- | --- | --- |
| str | EXTRA_STOCK_A | 上交所A股、深交所A股和北交所的股票列表 |
| str | SH_A | 上交所A股的股票列表 |
| str | SZ_A | 深交所A股的股票列表 |
| str | BJ_A | 北交所的股票列表 |
| str | EXTRA_STOCK_A_SH_SZ | 上交所A股和深交所A股的股票列表 |
| str | EXTRA_INDEX_A_SH_SZ | 上交所和深交所指数列表 |
| str | EXTRA_INDEX_A | 上交所、深交所和北交所的指数列表 |
| str | SH_INDEX | 上交所指数列表 |
| str | SZ_INDEX | 深交所指数列表 |
| str | BJ_INDEX | 北交所的指数列表 |
| str | SH_ETF | 上交所的ETF列表 |

| str | SZ_ETF | 深交所的ETF列表 |
| --- | --- | --- |
| str | EXTRA_ETF | 上交所、深交所的ETF列表 |
| str | SH_KZZ | 上交所的可转债列表 |
| str | SZ_KZZ | 深交所的可转债列表 |
| str | EXTRA_KZZ | 上交所、深交所的可转债列表 |
| str | SH_HKT | 沪港通 |
| str | SZ_HKT | 深港通 |
| str | EXTRA_HKT | 沪深港通 |
| str | SH_GLRA | 上交所逆回购 |
| str | SZ_GLRA | 深交所逆回购 |
| str | EXTRA_ GLRA | 沪深逆回购 |

代码类型 security_type(期货交易所)
4.1.2

| 数据类型 | 枚举值 | 说明 |
| --- | --- | --- |
| str | EXTRA_FUTURE | 期货, 包含中金所/上期所/大商所/郑商所/上海 国际能源交易中心所 |
| str | ZJ_FUTURE | 期货, 包含中金所 |
| str | SQ_FUTURE | 期货, 包含上期所 |
| str | DS_FUTURE | 期货, 包含大商所 |
| str | ZS_FUTURE | 期货, 包含郑商所 |
| str | SN_FUTURE | 期货, 包含海国际能源交易中心所 |

代码类型 security_type(期权)
4.1.3

| 数据类型 | 枚举值 | 说明 |
| --- | --- | --- |
| str | EXTRA_ETF_OP | ETF期权, 上交所/深交所 |
| str | SH_OPTION | ETF期货, 包含上交所 |
| str | SZ_OPTION | ETF期货, 包含深交所 |

### 4.1.4 市场类型 market

| 数据类型 | 枚举值 | 说明 |
| --- | --- | --- |
| str | SH | 上交所 |
| str | SZ | 深交所 |
| str | BJ | 北交所 |
| str | SHF | 上期所 |
| str | CFE | 中金所 |
| str | DCE | 大商所 |
| str | CZC | 郑商所 |

| str | INE | 上海国际能源交易中心所 |
| --- | --- | --- |
| str | SHN | 沪港通 |
| str | SZN | 深港通 |
| str | HK | 港交所 |

### 4.1.5 交易阶段代码 trading_phase_code

（1） 上海现货快照交易状态
该字段为8位字符数组,左起每位表示特定的含义,无定义则填空格。
第 0 位: ‘S’表示启动(开市前)时段,‘C’表示开盘集合竞价时段,‘T’表示连续交易时段,‘E’表示
闭市时段,‘P’表示产品停牌。
第1位: ‘0’表示此产品不可正常交易,‘1’表示此产品可正常交易。
第2位: ‘0’表示未上市,‘1’表示已上市。
第3位: ‘0’表示此产品在当前时段不接受进行新订单申报,‘1’ 表示此产品在当前时段可接受
进行新订单申报。

（2） 深圳现货快照交易状态
第 0位: 启动(开市前)‘O’= 开盘集合竞价‘T’= 连续竞价‘B’= 休市‘C’= 收盘集合竞价
‘S’=
已闭市‘H’= 临时停牌‘A’= 盘后交易‘V’=波动性中断。
‘E’=
第 1位: ‘0’= 正常状态 ‘1’= 全天停牌。交易阶段代码

（3） 港股股票行情交易状态
‘1’表示正常交易，‘2’表示停牌，‘3’表示复牌
（4） 上海期权快照交易状态

第 1 位： ‘S’表示启动（开市前）时段， ‘C’表示集合竞价时段，‘T’表示连续交易时段，

‘B’表示休市时段， ‘E’表示闭市时段， ‘V’表示波动性中断， ‘P’表示临时停牌、 ‘U’表示
收盘集合竞价。 ‘M’表示可恢复交易的熔断（盘中集合竞价） ,‘N’表示不可恢复交易的熔

断（暂停交易至闭市）；

第 2 位： ‘0’表示未连续停牌，‘1’表示连续停牌。（预留，暂填空格）；

第 3 位： ‘0’表示不限制开仓，‘1’表示限制备兑开仓， ‘2’表示卖出开仓， ‘3’表示限制

卖出开仓、备兑开仓， ‘4’表示限制买入开仓， ‘5’表示限制买入开仓、备兑开仓， ‘6’表示
限制买入开仓、卖出开仓， ‘7’表示限制买入开仓、卖出开仓、备兑开仓；

第 位： ‘0’表示此产品在当前时段不接受进行新订单申报，‘1’ 表示此产品在当前时段
4
可接受进行新订单申报。

### 4.1.6 产品状态标志 security_status

| 状态 | 标志 | 说明 |
| --- | --- | --- |
| 停牌 | 1 | 深交所、北交所 |
| 除权 | 2 | 上交所、深交所、北交所 |
| 除息 | 3 | 上交所、深交所、北交所 |
| 风险警示 | 4 | 上交所、深交所、北交所 |
| 退市整理期 | 5 | 上交所、深交所、北交所 |
| 上市首日 | 6 | 上交所、深交所、北交所 |

| 公司再融资 | 7 | 深交所 |
| --- | --- | --- |
| 恢复上市首日 | 8 | 深交所、北交所 |
| 网络投票 | 9 | 深交所 |
| 增发股份上市 | 10 | 深交所 |
| 合约调整 | 11 | 深交所 |
| 暂停上市后协议转让 | 12 | 深交所 |
| 实施双转单调整 | 13 | 深交所 |
| 特定债券转让 | 14 | 深交所、北交所 |
| 上市初期 | 15 | 深圳有效 |
| 退市整理期首日 | 16 | 深交所、北交所 |
| 新增股份 | 57 | 北交所 |
| 是否可作为融资融券可充抵 保证金证券 | 62 | 北交所 |
| 是否为融资标的 | 63 | 北交所 |
| 是否为融券标的 | 64 | 北交所 |
| 是否可质押入库 | 65 | 北交所 |
| 是否跨市场 | 66 | 北交所 |
| 是否处于转股回售期 | 67 | 北交所 |

数据周期
4.1.7 Period

| 数据类型 | 枚举值 | 说明 |
| --- | --- | --- |
| int | Period.min1.value | 1分钟线 |
| int | Period.min3.value | 3分钟线 |
| int | Period.min5.value | 5分钟线 |
| int | Period.min10.value | 10分钟线 |
| int | Period.min15.value | 15分钟线 |
| int | Period.min30.value | 30分钟线 |
| int | Period.min60.value | 60分钟线 |
| int | Period.min120.value | 120分钟线 |
| int | Period.day.value | 日线 |
| int | Period.week.value | 周线 |
| int | Period.month.value | 月线 |
| int | Period.season.value | 季度线 |
| int | Period.year.value | 年线 |

### 4.1.8 报告期名称 REPORT_TYPE

| 报告期类型代码 | 报告期月份 |
| --- | --- |
| 1 | 3月 |

| 2 | 6月 |
| --- | --- |
| 3 | 9月 |
| 4 | 12月 |

### 4.1.9 报表类型代码表 STATEMENT_TYPE

| 报表类型代码 | 报表类型 | 备注 |
| --- | --- | --- |
| 1 | 合并报表 | 涵盖母公司的财务报表数据，为最新报表 |
| 2 | 合并报表(单季 度) | 合并报表(单季度)=合并报表(本期)-合并报表(上一季) |
| 3 | 合并报表(单季 度调整) | 合并报表(单季度调整)=合并报表(本期调整)-合并报表 (上一季调整) |
| 4 | 合并报表(调整) | 本年度公布上年同期的财务报表数据，报告期为上年度 |
| 5 | 合并报表(更正 前) | 即出更正公告后，把合并报表的记录修改为合并报表(更 正前)；复制原来的记录，更正后报表类型改为合并报表 |
| 6 | 母公司报表 | 该公司母公司的财务报表数据 |
| 7 | 母公司报表(单 季度) | 母公司报表(单季度)=母公司报表(本期)-母公司报表(上 一季) |
| 8 | 母公司报表(单 季度调整) | 母公司报表(单季度调整)=母公司报表(本期调整)-母公 司报表(上一季调整) |
| 9 | 母公司报表(调 整) | 该公司母公司的本年度公布上年同期的财务报表数据 |
| 10 | 母公司报表(更 正前) | 之前上市公司已披露财务报表数据，但是由于某些特定 原因导致出错，未调整之前的原始财务报表数据。 |
| 11 | 合并报表(未公 开) | 未在公开信息源披露的财报且加工为合并报表口径 |
| 12 | 合并报表(调整 未公开) | 未在公开信息源披露的财报且加工为合并报表调整口径 |
| 13 | 合并报表(单季 度未公开) | 未在公开信息源披露的财报且加工为合并报表单季度口 径 |
| 14 | 合并报表(单季 度调整未公开) | 未在公开信息源披露的财报且加工为母公司报表口径 |
| 15 | 母公司报表(未 公开) | 未在公开信息源披露的财报且加工为母公司报表口径 |
| 16 | 母公司报表(调 整未公开) | 未在公开信息源披露的财报且加工为母公司报表调整口 径 |
| 17 | 母公司报表(单 季度未公开) | 未在公开信息源披露的财报且加工或计算为母公司报表 单季度口径 |
| 18 | 母公司报表(单 季度调整未公 开) | 未在公开信息源披露的财报且加工或计算为母公司报表 单季度调整口径 |
| 19 | 合并报表(调整 | 借壳前的合并报表(调整) |

- 借壳前)
- 20
- 21
- 22
- 23
- 24
- 25
- 26
- 27
- 28
- 29
- 30
- 31
- 32
- 33
- 34
- 35
- 36
- 37
- 38
- 39
- 40

| 41 | 合并报表(业绩 快报) | 加工业绩快报中的财务数据（海外数据专用） |
| --- | --- | --- |
| 42 | 合并调整(第一 次) | 第一次合并调整数据 |
| 43 | 合并调整(第二 次) | 第二次合并调整数据 |
| 44 | 合并调整(第三 次) | 第三次合并调整数据 |
| 45 | 合并报表(第四 次更正) | 有多次更正时，合并报表的第四次更正 |
| 46 | 合并调整(第四 次更正) | 有多次更正时，合并调整的第四次更正 |
| 47 | 母公司报表(第 四次更正) | 有多次更正时，母公司报表的第四次更正 |
| 48 | 母公司调整(第 四次更正) | 有多次更正时，母公司调整的第四次更正 |
| 50 | 合并调整(更正 前) | 即出更正公告后，把合并报表（调整）的记录修改为合 并调整(更正前)；复制原来的记录，更正后报表类型改 为合并报表(调整) |
| 51 | 合并报表(下半 年报) | 合并下半年度的报表 |
| 60 | 母公司调整(更 正前) | 该公司母公司的本年度公布上年同期的财务报表数据， 但是由于某些特定原因导致出错，未调整之前的原始财 务报表数据。 |
| 70 | 合并报表(借壳 前) | 公司主体在借壳上市前披露或者计算的为合并报表口径 的报表类型 |
| 80 | 合并报表(预测)81 | REITS基金的定期报告中披露的预测的合并报表数据合并报表(公司 预测) |
| 90 | 项目资产报表91 | 由项目资产管理人编制的一种财务报表，用于反映项目 资产的财务状况和经营情况合并报表(日历 年) |

股票分红进度代码表
4.1.10 DIV_PROGRESS

| 分红进度描述 | 进度代码 |
| --- | --- |
| 董事会预案 | 1 |
| 股东大会通过 | 2 |
| 实施 | 3 |
| 未通过 | 4 |
| 停止实施 | 12 |

| 股东提议 | 17 |
| --- | --- |
| 董事会预案预披露 | 19 |

分红实施进程：股东提议--董事会预案--股东大会--实施

### 4.1.11 股票配股进度代码表 PROGRESS

| 配股进度描述 | 进度代码 |
| --- | --- |
| 董事会预案 | 1 |
| 股东大会通过 | 2 |
| 实施 | 3 |
| 未通过 | 4 |
| 证监会核准 | 5 |
| 达成转让意向 | 6 |
| 签署转让协议 | 7 |
| 国资委批准 | 8 |
| 商务部批准 | 9 |
| 过户 | 10 |
| 延期实施 | 11 |
| 停止实施 | 12 |
| 分红方案待定 | 13 |
| 传闻 | 14 |
| 证监会受理 | 15 |
| 传闻被否认 | 16 |
| 股东提议 | 17 |
| 保监会批复 | 18 |
| 董事会预案预披露 | 19 |
| 发审委通过 | 20 |
| 发审委未通过 | 21 |
| 股东大会未通过 | 22 |
| 银监会批准 | 23 |
| 证监会恢复审核 | 24 |
| 预发行 | 25 |
| 提交注册 | 26 |

数据结构说明
4.2

快照
4.2.1 Level-1 Snapshot

| 数据类型 | 字段名称 | 说明 |
| --- | --- | --- |
| str | code | 证券代码+市场 |

| datetime | trade_time | 交易所行情数据时间 |
| --- | --- | --- |
| float | pre_close | 昨收价 |
| float | last | 最新价 |
| float | open | 开盘价 |
| float | high | 最高价 |
| float | low | 最低价 |
| float | close | 收盘价 |
| float | volume | 成交总量 |
| float | amount | 成交总金额 |
| float | num_trades | 成交笔数 |
| float | high_limited | 涨停价 |
| float | low_limited | 跌停价 |
| float | ask_price1 | 卖1档价格 |
| float | ask_price2 | 卖2档价格 |
| float | ask_price3 | 卖3档价格 |
| float | ask_price4 | 卖4档价格 |
| float | ask_price5 | 卖5档价格 |
| int | ask _volume1 | 卖1档量 |
| int | ask _volume2 | 卖2档量 |
| int | ask _volume3 | 卖3档量 |
| int | ask _volume4 | 卖4档量 |
| int | ask _volume5 | 卖5档量 |
| float | bid_price1 | 买1档价格 |
| float | bid_price2 | 买2档价格 |
| float | bid_price3 | 买3档价格 |
| float | bid_price4 | 买4档价格 |
| float | bid_price5 | 买5档价格 |
| int | bid _volume1 | 买1档量 |
| int | bid _volume2 | 买2档量 |
| int | bid _volume3 | 买3档量 |
| int | bid _volume4 | 买4档量 |
| int | bid _volume5 | 买5档量 |
| float | iopv | 净值估产（仅基金品种有效） |
| str | trading_phase_code | 交易阶段代码 |

期权快照
4.2.2 ETF SnapshotOption

| 数据类型 | 字段名称 | 说明 |
| --- | --- | --- |
| str | code | 证券代码+市场 |
| datetime | trade_time | 交易所行情数据时间 |
| str | trading_phase_code | 交易阶段代码 |
| int | total_long_position | 总持仓量 |

| float | volume | 成交总量 |
| --- | --- | --- |
| float | amount | 成交总金额 |
| float | pre_close | 昨收价 |
| float | pre_settle: | 上次结算价 |
| float | auction_price | 动态参考价（波动性中断参考价，仅上海有效）， |
| int | auction_volume | 虚拟匹配数量（仅上海有效） |
| float | last | 最新价 |
| float | open | 开盘价 |
| float | high | 最高价 |
| float | low | 最低价 |
| float | close | 收盘价 |
| float | settle | 本次结算价 |
| float | high_limited | 涨停价 |
| float | low_limited | 跌停价 |
| float | ask_price1 | 卖1档价格 |
| float | ask_price2 | 卖2档价格 |
| float | ask_price3 | 卖3档价格 |
| float | ask_price4 | 卖4档价格 |
| float | ask_price5 | 卖5档价格 |
| int | ask _volume1 | 卖1档量 |
| int | ask _volume2 | 卖2档量 |
| int | ask _volume3 | 卖3档量 |
| int | ask _volume4 | 卖4档量 |
| int | ask _volume5 | 卖5档量 |
| float | bid_price1 | 买1档价格 |
| float | bid_price2 | 买2档价格 |
| float | bid_price3 | 买3档价格 |
| float | bid_price4 | 买4档价格 |
| float | bid_price5 | 买5档价格 |
| int | bid _volume1 | 买1档量 |
| int | bid _volume2 | 买2档量 |
| int | bid _volume3 | 买3档量 |
| int | bid _volume4 | 买4档量 |
| int | bid _volume5 | 买5档量 |
| str | contract_type | 合约类别 |
| int | expire_date | 到期日 |
| str | underlying_security_cod | 标的代码 |
| float | exercise_price | 行权价 |

期货快照
4.2.3 SnapshotFuture

| 数据类型 | 字段名称 | 说明 |
| --- | --- | --- |
| str | code | 证券代码+市场 |
| datetime | trade_time | 交易所行情数据时间 |
| str | action_day | 业务日期 |
| str | trading_day | 交易日期 |
| float | pre_close | 昨收价 |
| float | pre_settle: | 上次结算价 |
| int | pre_open_interest | 昨持仓量 |
| int | open_interest | 持仓量 |
| float | last | 最新价 |
| float | open | 开盘价 |
| float | high | 最高价 |
| float | low | 最低价 |
| float | close | 收盘价 |
| float | volume | 成交总量 |
| float | amount | 成交总金额 |
| float | high_limited | 涨停价 |
| float | low_limited | 跌停价 |
| float | ask_price1 | 卖1档价格 |
| float | ask_price2 | 卖2档价格 |
| float | ask_price3 | 卖3档价格 |
| float | ask_price4 | 卖4档价格 |
| float | ask_price5 | 卖5档价格 |
| int | ask _volume1 | 卖1档量 |
| int | ask _volume2 | 卖2档量 |
| int | ask _volume3 | 卖3档量 |
| int | ask _volume4 | 卖4档量 |
| int | ask _volume5 | 卖5档量 |
| float | bid_price1 | 买1档价格 |
| float | bid_price2 | 买2档价格 |
| float | bid_price3 | 买3档价格 |
| float | bid_price4 | 买4档价格 |
| float | bid_price5 | 买5档价格 |
| int | bid _volume1 | 买1档量 |
| int | bid _volume2 | 买2档量 |
| int | bid _volume3 | 买3档量 |
| int | bid _volume4 | 买4档量 |
| int | bid _volume5 | 买5档量 |
| float | average_price | 当日均价 |
| float | settle | 本次结算价 |

指数快照
4.2.4 SnapshotIndex

| 数据类型 | 字段名称 | 说明 |
| --- | --- | --- |
| str | code | 证券代码+市场 |
| datetime | trade_time | 交易所行情数据时间 |
| float | last | 最新价 |
| float | pre_close | 前收盘价 |
| float | open | 今开盘价 |
| float | high | 最高价 |
| float | low | 最低价 |
| float | close | 收盘价（仅上海有效） |
| int | volume | 成交总量（上交所:手，深交所:张） |
| float | amount | 成交总金额 |

### 4.2.5 港股通快照 SnapshotHKT

| 数据类型 | 字段名称 | 说明 |
| --- | --- | --- |
| str | code | 证券代码+市场 |
| datetime | trade_time | 交易所行情数据时间 |
| float | pre_close | 昨收价 |
| float | last | 最新价 |
| float | high | 最高价 |
| float | low | 最低价 |
| float | volume | 成交总量 |
| float | amount | 成交总金额 |
| float | nominal_price | 暗盘价 |
| float | ref_price | 参考价 |
| float | bid_price_limit_up | 买盘上限价 |
| float | bid_price_limit_down | 买盘下限价 |
| float | offer_price_limit_up | 卖盘上限价 |
| float | offer_price_limit_down | 卖盘下限价 |
| float | high_limited | 冷静期价格上限 |
| float | low_limited | 冷静期价格下限 |
| float | ask_price1 | 卖1档价格 |
| float | ask_price2 | 卖2档价格 |
| float | ask_price3 | 卖3档价格 |
| float | ask_price4 | 卖4档价格 |
| float | ask_price5 | 卖5档价格 |
| int | ask _volume1 | 卖1档量 |

| int | ask _volume2 | 卖2档量 |
| --- | --- | --- |
| int | ask _volume3 | 卖3档量 |
| int | ask _volume4 | 卖4档量 |
| int | ask _volume5 | 卖5档量 |
| float | bid_price1 | 买1档价格 |
| float | bid_price2 | 买2档价格 |
| float | bid_price3 | 买3档价格 |
| float | bid_price4 | 买4档价格 |
| float | bid_price5 | 买5档价格 |
| int | bid _volume1 | 买1档量 |
| int | bid _volume2 | 买2档量 |
| int | bid _volume3 | 买3档量 |
| int | bid _volume4 | 买4档量 |
| int | bid _volume5 | 买5档量 |
| str | trading_phase_code | 交易阶段代码 |

### 4.2.6 K 线 Kline

| 数据类型 | 字段名称 | 说明 |
| --- | --- | --- |
| str | code | 证券代码+市场 |
| datetime | trade_time | 交易所行情数据时间 |
| float | open | 今开盘价 |
| float | high | 最高价 |
| float | low | 最低价 |
| float | close | 收盘价 |
| int | volume | 成交总量 |
| float | amount | 成交总金额 |

## 4.3 相关算法说明

### 4.3.1 商品期货查询算法

当查询非中金所（大商所、郑商所、上期所、上期能源）的商品期货快照时，因涉及夜
盘快照，需根据查询时间参数做相应区分，查询上以 20:00 作为夜盘的分割时间点，处理
逻辑见下表。

| 归属T-1日范围20:00:00.000~23:59:59.999 | 归属T日范围：00:00:00.000~19:59:59.999 |
| --- | --- |
| TGW 上送日 期 | 开始时间 |
| 20220407 | 093000000 |

- <结束时间，为有效查询，返回[4 月 7 日 9:30, 4月7日15:00]的数据
- 20220407
- 20220407
- 正常周一（未 跨 法 定 假 节 日）
- 特殊日（跨法 定假节日）
- 20220407
- 20220407
- 20220407

### 4.3.2 K 线算法说明

（1） 集合竞价的处理
对于分钟 K 线，开盘集合竞价数据的成交量包含在当日第一根 K 线，收盘集合竞
价数据的成交量包含在当日最后一根K线。
（2） 前推算法
9:30的1分钟K线，计算的是9:30:00.000~9:30:59.999期间的K线。

9:35的5分钟K线，计算的是9:35:00.000~9:39:59.999期间的K线。

本地数据缓存方案说明
4.4

应用场景：

（1） 接口取全量历史时间区间的数据

查询接口包含 local_path 和 is_local 两个参数的接口，这两个参数必须同时配对使用，支持
此本地缓存方案，本地保存全量历史数据，且每次调用接口默认增量更新本地数据，从而加

速接口读取速度；

（2） 接口取指定时间区间的数据

查询接口包含begin_date和end_date两个参数的接口，这两个参数必须同时配对使用，仅从

服务器获取数据，不本地缓存数据，速度较慢，且无增量更新机制。

### 4.4.1 函数入参说明

- local_path和is_local为参数组1，begin_date和end_date为参数组2；
- 一个参数组内的参数必须同时使用；
- 两个参数组需独立使用，即使用参数组 1 时，参数组 2 无效；使用参数组 2 时，参数组 1
- 无效。
- （1）local_path
- 类似'D://AmazingData_local_data//'，只写文件夹的绝对路径即可
- （2）is_local
- True:
- 本地local_path有数据的情况下，从本地取数据，但无法从服务端获取最新的数据
- 本地local_path无数据的情况下，从互联网取数据，并更新本地local_path的数据
- False:从互联网取数据，并更新本地local_path的数据
- （3） begin_date, end_date
- 开始日期、结束日期，在不同的接口中代表交易日、公告期等不同含义，具体见接口说明；
- 即按照日期从服务端取数据，不从本地取数据（即local_path和is_local两个参数无效）。

开始日期、结束日期，在不同的接口中代表交易日、公告期等不同含义，具体见接口说明；

### 4.4.2 本地存储文件说明

文件格式为hdf5格式

本地存储空间说明
4.4.3

本地存储空间，不同的数据类型和标的范围，所需空间不同。

建议本地存储空间在500GB以上。

5. 免责声明

为了使客户更好地了解使用中国银河证券股份有限公司(以下简称“本公司”)星耀

数智服务平台 (以下简称“本平台”)的相关风险，根据相关法律、行政法规、部门规

章、自律组织规则和监管规定，特提供风险揭示书，请客户务必详细阅读并充分理解以

下风险：

（1） 本公司使用外购或者自有的数据源作为基础数据进行数据加工、计算和分析，

但并不能保证数据的及时性、准确性、真实性和完整性。

（2） 由于计算机故障以及互联网数据传输等原因，数据传输可能会出现中断、停顿、

延迟、数据错误等情况；因特网和移动通讯网络遭到黑客恶意攻击、您的网络

终端设备及软件系统收到非法攻击或病毒感染、您的网络终端设备及软件系统

与本不兼容、因电脑的故障或互联网故障引起的中断和错误等，都可能会造成

数据传输故障，由此导致的损失由您自行承担；

（3） 本平台所提供的信息数据等全部内容仅供参考，投资者须自行确认自己具备理

解相关信息数据内容的专业能力，保持自身的独立判断，任何情况下本平台提

供的内容不构成对投资者的投资建议，据此操作的一切风险和损失由投资者自

行承担，本公司不对任何人因参考上述内容造成的直接或间接损失或与此有关

的其他损失承担任何责任。

（4） 您使用本平台过程中，凡使用您本人的用户名和密码，针对平台账号进行的操

作均视为您亲自办理，由此所产生的一切后果由您承担。本公司提醒您加强账

号、密码等信息的保护工作，不得出借他人使用，并建议您定期修改密码、增

强密码强度、防止密码泄露、及时查询交易记录、防止用于网上交易的计算机

或手机终端感染木马、病毒等。如果本公司发现同一使用账号和验证码在同一

时间内被多人同时登录使用，本公司有权停止向您提供本平台相关服务，且不

承担任何责任。

（5） 由于地震、水灾、火灾等不可抗力因素或者无法控制和不可预测的系统故障、

设备故障、通讯故障、电力故障、网络故障及其它因素，可能本平台非正常运

行甚至瘫痪，出现信息异常或信息传递异常等情况，由此产生的损失将由您承

担。

（6） 本公司可能不时更新或升级本平台，您应按照本公司的技术要求在规定的时间

内配合做好更新或升级工作；因您未按本公司通知要求进行变更、升级的，由

此发生的任何损失由您自行承担。

（7） 如果本公司依据自身判断认为您违反本平台相关的国家法律法规、规范性文件，

以及证券交易所、行业协会等自律组织的规则和要求(以下合称“法律法规”)，

且不按法律法规或乙方要求及时纠正的，或影响本公司信息系统安全运行的，

或监管机构、交易所、行业自律组织对本平台提出监管要求或相关业务规则发

生变化，可能导致本平台的服务形式发生变化或本公司决定完全停止提供该项

服务的，本公司有权立即停止您使用本平台，并且不承担任何责任，由此产生

的任何损失由您承担。

（8） 本公司在遵守国家相关法律、法规、规章及自律组织规则、监管政策前提下，

尽力为客户提供高速、完整、准确的金融数据服务，但因受制于数据来源、技

术能力等多种因素影响，本公司不保证数据源的及时性、准确性或者完整性，

因数据源的遗漏、错误、丢失、延迟、中断而可能造成的损失将由您承担，本

公司不承担任何责任。

（9） 本平台的相关用户文档仅供您操作参考，如您对于本平台的使用不熟悉，可能

因操作不当造成本平台出现非正常现象，上述风险可能导致发生的损失应由您

自身承担，本公司不承担任何责任。

（10） 您申请使用本平台前应如实填写相关信息和资料，使用过程中信息资料发生变

更应及时告知本公司，因您未及时、准确、完整地提供或变更相关信息和资料，

导致本公司不能及时、有效地为您提供服务，或导致本公司依据不准确、不完

整的信息提供服务，由此可能造成的损失由您自行承担。

（11） 对于客户未及时更新信息，或者不再符合本平台使用条件，或本平台权限期限

到期，或存在重大风险隐患，公司认为不适合使用星耀数智服务平台时，公司

可关闭客户的系统相关权限，由此导致的损失由您自行承担。

（12） 本公司开发的本平台及本平台提供的相关数据知识产权归本公司所有。本公司

为您开通本平台账号后，仅供您个人使用，如您把本平台提供的全部或部分资

料和数据以任何形式转移、出售和公开给任何第三人，或因您未采取必要和合

适的措施保护本平台提供的资料和数据的知识产权而造成数据资料信息泄露给

任何第三人，本公司有权暂停或终止您使用本平台，由此导致的损失由您自行

承担。

（13） 本免责声明无法揭示您使用本平台及通过本平台从事投资交易的所有风险，故

您在使用本平台之前，应全面了解相关法律法规及有关规定，对您自身的经济

承受能力、风险承受能力、投资目标、风险控制能力等综合考虑，作出客观判

断，对投资交易作仔细的研究。
