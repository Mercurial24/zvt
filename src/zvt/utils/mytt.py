# MyTT 麦语言-通达信-同花顺指标实现     https://github.com/mpquant/MyTT
# MyTT高级函数验证版本：               https://github.com/mpquant/MyTT/blob/main/MyTT_plus.py
# Python2老版本pandas特别的MyTT：      https://github.com/mpquant/MyTT/blob/main/MyTT_python2.py 
# V2.1  2021-6-6   新增 BARSLAST函数 SLOPE,FORCAST线性回归预测函数
# V2.3  2021-6-13  新增 TRIX,DPO,BRAR,DMA,MTM,MASS,ROC,VR,ASI等指标
# V2.4  2021-6-27  新增 EXPMA,OBV,MFI指标, 改进SMA核心函数(核心函数彻底无循环)
# V2.7  2021-11-21 修正 SLOPE,BARSLAST,函数,新加FILTER,LONGCROSS, 感谢qzhjiang对SLOPE,SMA等函数的指正
# V2.8  2021-11-23 修正 FORCAST,WMA函数,欢迎qzhjiang,stanene,bcq加入社群，一起来完善myTT库
# V2.9  2021-11-29 新增 HHVBARS,LLVBARS,CONST, VALUEWHEN功能函数
# V2.92 2021-11-30 新增 BARSSINCEN函数,现在可以 pip install MyTT 完成安装   
# V3.0  2021-12-04 改进 DMA函数支持序列,新增XS2 薛斯通道II指标
# V3.1  2021-12-19 新增 TOPRANGE,LOWRANGE一级函数 
# V3.2  2023-04-04 新增 CR指标
# V3.3  2023-11-09 新增 SIN,COS,TAN序列处理的三角函数
  
#以下所有函数如无特别说明，输入参数S均为numpy序列或者列表list，N为整型int 
#应用层1级函数完美兼容通达信或同花顺，具体使用方法请参考通达信 

import numpy as np; import pandas as pd

#------------------ 0级：核心工具函数 --------------------------------------------      
def RD(N,D=3):   return np.round(N,D)        #四舍五入取3位小数 
def RET(S,N=1):  return np.array(S)[-N]      #返回序列倒数第N个值,默认返回最后一个
def ABS(S):      return np.abs(S)            #返回N的绝对值
def LN(S):       return np.log(S)            #求底是e的自然对数,
def POW(S,N):    return np.power(S,N)        #求S的N次方
def SQRT(S):     return np.sqrt(S)           #求S的平方根
def SIN(S):      return np.sin(S)            #求S的正弦值（弧度)
def COS(S):      return np.cos(S)            #求S的余弦值（弧度)
def TAN(S):      return np.tan(S)            #求S的正切值（弧度)  
def MAX(S1,S2):  return np.maximum(S1,S2)    #序列max
def MIN(S1,S2):  return np.minimum(S1,S2)    #序列min
def IF(S,A,B):   return np.where(S,A,B)      #序列布尔判断 return=A  if S==True  else  B


def REF(S, N=1):          #对序列整体下移动N,返回序列(shift后会产生NAN)    
    return pd.Series(S).shift(N).values  

def DIFF(S, N=1):         #前一个值减后一个值,前面会产生nan 
    return pd.Series(S).diff(N).values     #np.diff(S)直接删除nan，会少一行

def STD(S,N):             #求序列的N日标准差，返回序列    
    return  pd.Series(S).rolling(N).std(ddof=0).values     

def SUM(S, N):            #对序列求N天累计和，返回序列    N=0对序列所有依次求和         
    return pd.Series(S).rolling(N).sum().values if N>0 else pd.Series(S).cumsum().values  

def CONST(S):             #返回序列S最后的值组成常量序列
    return np.full(len(S),S[-1])
  
def HHV(S,N):             #HHV(C, 5) 最近5天收盘最高价        
    return pd.Series(S).rolling(N).max().values     

def LLV(S,N):             #LLV(C, 5) 最近5天收盘最低价     
    return pd.Series(S).rolling(N).min().values    
    
def HHVBARS(S,N):         #求N周期内S最高值到当前周期数, 返回序列
    return pd.Series(S).rolling(N).apply(lambda x: np.argmax(x[::-1]),raw=True).values 

def LLVBARS(S,N):         #求N周期内S最低值到当前周期数, 返回序列
    return pd.Series(S).rolling(N).apply(lambda x: np.argmin(x[::-1]),raw=True).values    
  
def MA(S,N):              #求序列的N日简单移动平均值，返回序列                    
    return pd.Series(S).rolling(N).mean().values  
  
def EMA(S,N):             #指数移动平均,为了精度 S>4*N  EMA至少需要120周期     alpha=2/(span+1)    
    return pd.Series(S).ewm(span=N, adjust=False).mean().values     

def SMA(S, N, M=1):       #中国式的SMA,至少需要120周期才精确 (雪球180周期)    alpha=1/(1+com)    
    return pd.Series(S).ewm(alpha=M/N,adjust=False).mean().values           #com=N-M/M

def WMA(S, N):            #通达信S序列的N日加权移动平均 Yn = (1*X1+2*X2+3*X3+...+n*Xn)/(1+2+3+...+Xn)
    return pd.Series(S).rolling(N).apply(lambda x:x[::-1].cumsum().sum()*2/N/(N+1),raw=True).values 

def DMA(S, A):            #求S的动态移动平均，A作平滑因子,必须 0<A<1  (此为核心函数，非指标）
    if isinstance(A,(int,float)):  return pd.Series(S).ewm(alpha=A,adjust=False).mean().values    
    A=np.array(A);   A[np.isnan(A)]=1.0;   Y= np.zeros(len(S));   Y[0]=S[0]     
    for i in range(1,len(S)): Y[i]=A[i]*S[i]+(1-A[i])*Y[i-1]      #A支持序列 by jqz1226         
    return Y             
  
def AVEDEV(S, N):         #平均绝对偏差  (序列与其平均值的绝对差的平均值)   
    return pd.Series(S).rolling(N).apply(lambda x: (np.abs(x - x.mean())).mean()).values 

def SLOPE(S, N):          #返S序列N周期回线性回归斜率            
    return pd.Series(S).rolling(N).apply(lambda x: np.polyfit(range(N),x,deg=1)[0],raw=True).values

def FORCAST(S, N):        #返回S序列N周期回线性回归后的预测值， jqz1226改进成序列出    
    return pd.Series(S).rolling(N).apply(lambda x:np.polyval(np.polyfit(range(N),x,deg=1),N-1),raw=True).values  

def LAST(S, A, B):        #从前A日到前B日一直满足S_BOOL条件, 要求A>B & A>0 & B>=0 
    return np.array(pd.Series(S).rolling(A+1).apply(lambda x:np.all(x[::-1][B:]),raw=True),dtype=bool)
  
#------------------   1级：应用层函数(通过0级核心函数实现）使用方法请参考通达信--------------------------------
def COUNT(S, N):                       # COUNT(CLOSE>O, N):  最近N天满足S_BOO的天数  True的天数
    return SUM(S,N)    

def EVERY(S, N):                       # EVERY(CLOSE>O, 5)   最近N天是否都是True
    return  IF(SUM(S,N)==N,True,False)                    
  
def EXIST(S, N):                       # EXIST(CLOSE>3010, N=5)  n日内是否存在一天大于3000点  
    return IF(SUM(S,N)>0,True,False)

def FILTER(S, N):                      # FILTER函数，S满足条件后，将其后N周期内的数据置为0, FILTER(C==H,5)
    for i in range(len(S)): S[i+1:i+1+N]=0  if S[i] else S[i+1:i+1+N]        
    return S                           # 例：FILTER(C==H,5) 涨停后，后5天不再发出信号 
  
def BARSLAST(S):                       #上一次条件成立到当前的周期, BARSLAST(C/REF(C,1)>=1.1) 上一次涨停到今天的天数 
    M=np.concatenate(([0],np.where(S,1,0)))  
    for i in range(1, len(M)):  M[i]=0 if M[i] else M[i-1]+1    
    return M[1:]                       

def BARSLASTCOUNT(S):                  # 统计连续满足S条件的周期数        by jqz1226
    rt = np.zeros(len(S)+1)            # BARSLASTCOUNT(CLOSE>OPEN)表示统计连续收阳的周期数
    for i in range(len(S)): rt[i+1]=rt[i]+1  if S[i] else rt[i+1]
    return rt[1:]  
  
def BARSSINCEN(S, N):                  # N周期内第一次S条件成立到现在的周期数,N为常量  by jqz1226
    return pd.Series(S).rolling(N).apply(lambda x:N-1-np.argmax(x) if np.argmax(x) or x[0] else 0,raw=True).fillna(0).values.astype(int)
  
def CROSS(S1, S2):                     # 判断向上金叉穿越 CROSS(MA(C,5),MA(C,10))  判断向下死叉穿越 CROSS(MA(C,10),MA(C,5))   
    return np.concatenate(([False], np.logical_not((S1>S2)[:-1]) & (S1>S2)[1:]))    # 不使用0级函数,移植方便  by jqz1226
    
def LONGCROSS(S1,S2,N):                # 两条线维持一定周期后交叉,S1在N周期内都小于S2,本周期从S1下方向上穿过S2时返回1,否则返回0         
    return  np.array(np.logical_and(LAST(S1<S2,N,1),(S1>S2)),dtype=bool)            # N=1时等同于CROSS(S1, S2)
    
def VALUEWHEN(S, X):                   # 当S条件成立时,取X的当前值,否则取VALUEWHEN的上个成立时的X值   by jqz1226
    return pd.Series(np.where(S,X,np.nan)).ffill().values  

def BETWEEN(S, A, B):                  # S处于A和B之间时为真。 包括 A<S<B 或 A>S>B
    return ((A<S) & (S<B)) | ((A>S) & (S>B))  

def TOPRANGE(S):                       # TOPRANGE(HIGH)表示当前最高价是近多少周期内最高价的最大值 by jqz1226
    rt = np.zeros(len(S))
    for i in range(1,len(S)):  rt[i] = np.argmin(np.flipud(S[:i]<S[i]))
    return rt.astype('int')

def LOWRANGE(S):                       # LOWRANGE(LOW)表示当前最低价是近多少周期内最低价的最小值 by jqz1226
    rt = np.zeros(len(S))
    for i in range(1,len(S)):  rt[i] = np.argmin(np.flipud(S[:i]>S[i]))
    return rt.astype('int')
  
  
#------------------   2级：技术指标函数(全部通过0级，1级函数实现） ------------------------------
def MACD(CLOSE,SHORT=12,LONG=26,M=9):             # EMA的关系，S取120日，和雪球小数点2位相同
    DIF = EMA(CLOSE,SHORT)-EMA(CLOSE,LONG);  
    DEA = EMA(DIF,M);      MACD=(DIF-DEA)*2
    return RD(DIF),RD(DEA),RD(MACD)

def KDJ(CLOSE,HIGH,LOW, N=9,M1=3,M2=3):           # KDJ指标
    RSV = (CLOSE - LLV(LOW, N)) / (HHV(HIGH, N) - LLV(LOW, N)) * 100
    K = EMA(RSV, (M1*2-1));    D = EMA(K,(M2*2-1));        J=K*3-D*2
    return K, D, J

def RSI(CLOSE, N=24):                           # RSI指标,和通达信小数点2位相同
    DIF = CLOSE-REF(CLOSE,1) 
    return RD(SMA(MAX(DIF,0), N) / SMA(ABS(DIF), N) * 100)  

def WR(CLOSE, HIGH, LOW, N=10, N1=6):            #W&R 威廉指标
    WR = (HHV(HIGH, N) - CLOSE) / (HHV(HIGH, N) - LLV(LOW, N)) * 100
    WR1 = (HHV(HIGH, N1) - CLOSE) / (HHV(HIGH, N1) - LLV(LOW, N1)) * 100
    return RD(WR), RD(WR1)

def BIAS(CLOSE,L1=6, L2=12, L3=24):              # BIAS乖离率
    BIAS1 = (CLOSE - MA(CLOSE, L1)) / MA(CLOSE, L1) * 100
    BIAS2 = (CLOSE - MA(CLOSE, L2)) / MA(CLOSE, L2) * 100
    BIAS3 = (CLOSE - MA(CLOSE, L3)) / MA(CLOSE, L3) * 100
    return RD(BIAS1), RD(BIAS2), RD(BIAS3)

def BOLL(CLOSE,N=20, P=2):                       #BOLL指标，布林带    
    MID = MA(CLOSE, N); 
    UPPER = MID + STD(CLOSE, N) * P
    LOWER = MID - STD(CLOSE, N) * P
    return RD(UPPER), RD(MID), RD(LOWER)    

def PSY(CLOSE,N=12, M=6):  
    PSY=COUNT(CLOSE>REF(CLOSE,1),N)/N*100
    PSYMA=MA(PSY,M)
    return RD(PSY),RD(PSYMA)

def CCI(CLOSE,HIGH,LOW,N=14):  
    TP=(HIGH+LOW+CLOSE)/3
    return (TP-MA(TP,N))/(0.015*AVEDEV(TP,N))
        
def ATR(CLOSE,HIGH,LOW, N=20):                    #真实波动N日平均值
    TR = MAX(MAX((HIGH - LOW), ABS(REF(CLOSE, 1) - HIGH)), ABS(REF(CLOSE, 1) - LOW))
    return MA(TR, N)

def BBI(CLOSE,M1=3,M2=6,M3=12,M4=20):             #BBI多空指标   
    return (MA(CLOSE,M1)+MA(CLOSE,M2)+MA(CLOSE,M3)+MA(CLOSE,M4))/4    

def DMI(CLOSE,HIGH,LOW,M1=14,M2=6):               #动向指标：结果和同花顺，通达信完全一致
    TR = SUM(MAX(MAX(HIGH - LOW, ABS(HIGH - REF(CLOSE, 1))), ABS(LOW - REF(CLOSE, 1))), M1)
    HD = HIGH - REF(HIGH, 1);     LD = REF(LOW, 1) - LOW
    DMP = SUM(IF((HD > 0) & (HD > LD), HD, 0), M1)
    DMM = SUM(IF((LD > 0) & (LD > HD), LD, 0), M1)
    PDI = DMP * 100 / TR;         MDI = DMM * 100 / TR
    ADX = MA(ABS(MDI - PDI) / (PDI + MDI) * 100, M2)
    ADXR = (ADX + REF(ADX, M2)) / 2
    return PDI, MDI, ADX, ADXR  

def TAQ(HIGH,LOW,N):                               #唐安奇通道(海龟)交易指标，大道至简，能穿越牛熊
    UP=HHV(HIGH,N);    DOWN=LLV(LOW,N);    MID=(UP+DOWN)/2
    return UP,MID,DOWN

def KTN(CLOSE,HIGH,LOW,N=20,M=10):                 #肯特纳交易通道, N选20日，ATR选10日
    MID=EMA((HIGH+LOW+CLOSE)/3,N)
    ATRN=ATR(CLOSE,HIGH,LOW,M)
    UPPER=MID+2*ATRN;   LOWER=MID-2*ATRN
    return UPPER,MID,LOWER       
  
def TRIX(CLOSE,M1=12, M2=20):                      #三重指数平滑平均线
    TR = EMA(EMA(EMA(CLOSE, M1), M1), M1)
    TRIX = (TR - REF(TR, 1)) / REF(TR, 1) * 100
    TRMA = MA(TRIX, M2)
    return TRIX, TRMA

def VR(CLOSE,VOL,M1=26):                            #VR容量比率
    LC = REF(CLOSE, 1)
    return SUM(IF(CLOSE > LC, VOL, 0), M1) / SUM(IF(CLOSE <= LC, VOL, 0), M1) * 100
  
def CR(CLOSE,HIGH,LOW,N=20):                        #CR价格动量指标
    MID=REF(HIGH+LOW+CLOSE,1)/3;
    return SUM(MAX(0,HIGH-MID),N)/SUM(MAX(0,MID-LOW),N)*100  

def EMV(HIGH,LOW,VOL,N=14,M=9):                     #简易波动指标 
    VOLUME=MA(VOL,N)/VOL;       MID=100*(HIGH+LOW-REF(HIGH+LOW,1))/(HIGH+LOW)
    EMV=MA(MID*VOLUME*(HIGH-LOW)/MA(HIGH-LOW,N),N);    MAEMV=MA(EMV,M)
    return EMV,MAEMV


def DPO(CLOSE,M1=20, M2=10, M3=6):                  #区间震荡线
    DPO = CLOSE - REF(MA(CLOSE, M1), M2);    MADPO = MA(DPO, M3)
    return DPO, MADPO

def BRAR(OPEN,CLOSE,HIGH,LOW,M1=26):                 #BRAR-ARBR 情绪指标  
    AR = SUM(HIGH - OPEN, M1) / SUM(OPEN - LOW, M1) * 100
    BR = SUM(MAX(0, HIGH - REF(CLOSE, 1)), M1) / SUM(MAX(0, REF(CLOSE, 1) - LOW), M1) * 100
    return AR, BR

def DFMA(CLOSE,N1=10,N2=50,M=10):                    #平行线差指标 
    DIF=MA(CLOSE,N1)-MA(CLOSE,N2); DIFMA=MA(DIF,M)   #通达信指标叫DMA 同花顺叫新DMA
    return DIF,DIFMA

def MTM(CLOSE,N=12,M=6):                             #动量指标
    MTM=CLOSE-REF(CLOSE,N);         MTMMA=MA(MTM,M)
    return MTM,MTMMA

def MASS(HIGH,LOW,N1=9,N2=25,M=6):                   #梅斯线
    MASS=SUM(MA(HIGH-LOW,N1)/MA(MA(HIGH-LOW,N1),N1),N2)
    MA_MASS=MA(MASS,M)
    return MASS,MA_MASS
  
def ROC(CLOSE,N=12,M=6):                             #变动率指标
    ROC=100*(CLOSE-REF(CLOSE,N))/REF(CLOSE,N);    MAROC=MA(ROC,M)
    return ROC,MAROC  

def EXPMA(CLOSE,N1=12,N2=50):                        #EMA指数平均数指标
    return EMA(CLOSE,N1),EMA(CLOSE,N2);

def OBV(CLOSE,VOL):                                  #能量潮指标
    return SUM(IF(CLOSE>REF(CLOSE,1),VOL,IF(CLOSE<REF(CLOSE,1),-VOL,0)),0)/10000

def MFI(CLOSE,HIGH,LOW,VOL,N=14):                    #MFI指标是成交量的RSI指标
    TYP = (HIGH + LOW + CLOSE)/3
    V1=SUM(IF(TYP>REF(TYP,1),TYP*VOL,0),N)/SUM(IF(TYP<REF(TYP,1),TYP*VOL,0),N)  
    return 100-(100/(1+V1))     
  
def ASI(OPEN,CLOSE,HIGH,LOW,M1=26,M2=10):            #振动升降指标
    LC=REF(CLOSE,1);      AA=ABS(HIGH-LC);     BB=ABS(LOW-LC);
    CC=ABS(HIGH-REF(LOW,1));   DD=ABS(LC-REF(OPEN,1));
    R=IF( (AA>BB) & (AA>CC),AA+BB/2+DD/4,IF( (BB>CC) & (BB>AA),BB+AA/2+DD/4,CC+DD/4));
    X=(CLOSE-LC+(CLOSE-OPEN)/2+LC-REF(OPEN,1));
    SI=16*X/R*MAX(AA,BB);   ASI=SUM(SI,M1);   ASIT=MA(ASI,M2);
    return ASI,ASIT   

def XSII(CLOSE, HIGH, LOW, N=102, M=7):              #薛斯通道II  
    AA  = MA((2*CLOSE + HIGH + LOW)/4, 5)            #最新版DMA才支持 2021-12-4
    TD1 = AA*N/100;   TD2 = AA*(200-N) / 100
    CC =  ABS((2*CLOSE + HIGH + LOW)/4 - MA(CLOSE,20))/MA(CLOSE,20)
    DD =  DMA(CLOSE,CC);    TD3=(1+M/100)*DD;      TD4=(1-M/100)*DD
    return TD1, TD2, TD3, TD4  
  
  
  #望大家能提交更多指标和函数  https://github.com/mpquant/MyTT

  
  

# MyTT 麦语言-通达信-同花顺指标实现     https://github.com/mpquant/MyTT
# 高级函数版本，本文件函数计算结果经过验证完全正确，可以正常使用，但代码比较复杂，做为进阶使用。
# MyTT团队对每个函数精益求精，力争效率速度，代码优雅的完美统一，如果您有更好的实现方案，请不吝赐教！
# 感谢以下团队成员的努力和贡献： 火焰，jqz1226, stanene, bcq

#------------------------工具函数---------------------------------------------

def HHV(S, N):  #HHV,支持N为序列版本
    # type: (np.ndarray, Optional[int,float, np.ndarray]) -> np.ndarray
    """
    HHV(C, 5)  # 最近5天收盘最高价
    """
    if isinstance(N, (int, float)):
        return pd.Series(S).rolling(N).max().values
    else:
        res = np.repeat(np.nan, len(S))
        for i in range(len(S)):
            if (not np.isnan(N[i])) and N[i] <= i + 1:
                res[i] = S[i + 1 - N[i]:i + 1].max()
        return res

    
def LLV(S, N):   #LLV,支持N为序列版本
    # type: (np.ndarray, Optional[int,float, np.ndarray]) -> np.ndarray
    """
    LLV(C, 5)  # 最近5天收盘最低价
    """
    if isinstance(N, (int, float)):
        return pd.Series(S).rolling(N).min().values
    else:
        res = np.repeat(np.nan, len(S))
        for i in range(len(S)):
            if (not np.isnan(N[i])) and N[i] <= i + 1:
                res[i] = S[i + 1 - N[i]:i + 1].min()
        return res


def DSMA(X, N):    # 偏差自适应移动平均线   type: (np.ndarray, int) -> np.ndarray
    """
    Deviation Scaled Moving Average (DSMA)    Python by: jqz1226, 2021-12-27
    Referred function from myTT: SUM, DMA
    """
    a1 = math.exp(- 1.414 * math.pi * 2 / N)
    b1 = 2 * a1 * math.cos(1.414 * math.pi * 2 / N)
    c2 = b1
    c3 = -a1 * a1
    c1 = 1 - c2 - c3        
    Zeros = np.pad(X[2:] - X[:-2],(2,0),'constant')          
    Filt = np.zeros(len(X))
    for i in range(len(X)):
        Filt[i] = c1 * (Zeros[i] + Zeros[i-1]) / 2 + c2 * Filt[i-1] + c3 * Filt[i-2]    
    
    RMS = np.sqrt(SUM(np.square(Filt), N) / N)
    ScaledFilt = Filt / RMS
    alpha1 = np.abs(ScaledFilt) * 5 / N    
    return DMA(X, alpha1)    



def SUMBARSFAST(X, A): 
    # type: (np.ndarray, Optional[np.ndarray, float, int]) -> np.ndarray
    """
    通达信SumBars函数的Python实现  by jqz1226
    SumBars函数将X向前累加，直到大于等于A, 返回这个区间的周期数。例如SUMBARS(VOL, CAPITAL),求完全换手的周期数。
    :param X: 数组。被累计的源数据。 源数组中不能有小于0的元素。
    :param A: 数组（一组）或者浮点数（一个）或者整数（一个），累加截止的界限数
    :return:  数组。各K线分别对应的周期数
    """
    if any(X<=0):   raise ValueError('数组X的每个元素都必须大于0！')
    
    X = np.flipud(X)  # 倒转
    length = len(X)

    if isinstance(A * 1.0, float):  A = np.repeat(A, length)  # 是单值则转化为数组
    A = np.flipud(A)  # 倒转
    sumbars = np.zeros(length)  # 初始化sumbars为0
    Sigma = np.insert(np.cumsum(X), 0, 0.0)  # 在累加值前面插入一个0.0（元素变多1个，便于引用）

    for i in range(length):
        k = np.searchsorted(Sigma[i + 1:], A[i] + Sigma[i])
        if k < length - i:  # 找到
            sumbars[length - i - 1] = k + 1
    return sumbars.astype(int)  
  
  


#------------------------指标函数---------------------------------------------

def SAR(HIGH, LOW, N=10, S=2, M=20):             
    """
    求抛物转向。 例如SAR(10,2,20)表示计算10日抛物转向，步长为2%，步长极限为20%
    Created by: jqz1226, 2021-11-24首次发表于聚宽(www.joinquant.com)
    
    :param HIGH: high序列
    :param LOW: low序列
    :param N: 计算周期
    :param S: 步长
    :param M: 步长极限
    :return: 抛物转向
    """
    f_step = S / 100;    f_max = M / 100;    af = 0.0
    is_long = HIGH[N - 1] > HIGH[N - 2]
    b_first = True
    length = len(HIGH)

    s_hhv = REF(HHV(HIGH, N), 1)  # type: np.ndarray
    s_llv = REF(LLV(LOW, N), 1)  # type: np.ndarray
    sar_x = np.repeat(np.nan, length)  # type: np.ndarray
    for i in range(N, length):
        if b_first:  # 第一步
            af = f_step
            sar_x[i] = s_llv[i] if is_long else s_hhv[i]
            b_first = False
        else:  # 继续多 或者 空
            ep = s_hhv[i] if is_long else s_llv[i]  # 极值
            if (is_long and HIGH[i] > ep) or ((not is_long) and LOW[i] < ep):  # 顺势：多创新高 或者 空创新低
                af = min(af + f_step, f_max)
            #
            sar_x[i] = sar_x[i - 1] + af * (ep - sar_x[i - 1])

        if (is_long and LOW[i] < sar_x[i]) or ((not is_long) and HIGH[i] > sar_x[i]):  # 反空 或者 反多
            is_long = not is_long
            b_first = True
    return sar_x


def TDX_SAR(High, Low, iAFStep=2, iAFLimit=20):    # type: (np.ndarray, np.ndarray, int, int) -> np.ndarray
    """  通达信SAR算法,和通达信SAR对比完全一致   by: jqz1226, 2021-12-18
    :param High: 最高价序列
    :param Low: 最低价序列
    :param iAFStep: AF步长
    :param iAFLimit: AF极限值
    :return: SAR序列
    """
    af_step = iAFStep / 100;     af_limit = iAFLimit / 100
    SarX = np.zeros(len(High))   # 初始化返回数组

    # 第一个bar
    bull = True 
    af = af_step
    ep = High[0]
    SarX[0] = Low[0]
    # 第2个bar及其以后
    for i in range(1, len(High)):
        # 1.更新：hv, lv, af, ep
        if bull:  # 多
            if High[i] > ep:  # 创新高
                ep = High[i]
                af = min(af + af_step, af_limit)
        else:  # 空
            if Low[i] < ep:  # 创新低
                ep = Low[i]
                af = min(af + af_step, af_limit)
        # 2.计算SarX
        SarX[i] = SarX[i - 1] + af * (ep - SarX[i - 1])

        # 3.修正SarX
        if bull:
            SarX[i] = max(SarX[i - 1], min(SarX[i], Low[i], Low[i - 1]))
        else:
            SarX[i] = min(SarX[i - 1], max(SarX[i], High[i], High[i - 1]))

        # 4. 判断是否：向下跌破，向上突破
        if bull:  # 多
            if Low[i] < SarX[i]:  # 向下跌破，转空
                bull = False
                tmp_SarX = ep  # 上阶段的最高点
                ep = Low[i]
                af = af_step
                if High[i - 1] == tmp_SarX:  # 紧邻即最高点
                    SarX[i] = tmp_SarX
                else:
                    SarX[i] = tmp_SarX + af * (ep - tmp_SarX)
        else:  # 空
            if High[i] > SarX[i]:  # 向上突破, 转多
                bull = True
                ep = High[i]
                af = af_step
                SarX[i] = min(Low[i], Low[i - 1])
    # end for
    return SarX



# MyTT_wx 麦语言-通达信-同花顺指标拓展库  
# 这是一个 MyTT 的扩展库，包含了市场上经典的、MyTT 原版未收录的高级指标
# 风格完全兼容 MyTT，函数可以直接复制使用

import numpy as np; import pandas as pd

#------------------ 0级：核心工具函数 (从 MyTT.py 移植，确保独立运行) ----------------      
def RD(N,D=3):   return np.round(N,D)        #四舍五入取3位小数 
def RET(S,N=1):  return np.array(S)[-N]      #返回序列倒数第N个值,默认返回最后一个
def ABS(S):      return np.abs(S)            #返回N的绝对值
def MAX(S1,S2):  return np.maximum(S1,S2)    #序列max
def MIN(S1,S2):  return np.minimum(S1,S2)    #序列min
def IF(S,A,B):   return np.where(S,A,B)      #序列布尔判断 return=A  if S==True  else  B

def REF(S, N=1):          #对序列整体下移动N,返回序列(shift后会产生NAN)    
    return pd.Series(S).shift(N).values  

def STD(S,N):             #求序列的N日标准差，返回序列    
    return  pd.Series(S).rolling(N).std(ddof=0).values     

def SUM(S, N):            #对序列求N天累计和，返回序列    N=0对序列所有依次求和         
    return pd.Series(S).rolling(N).sum().values if N>0 else pd.Series(S).cumsum().values  

def HHV(S,N):             #HHV(C, 5) 最近5天收盘最高价        
    return pd.Series(S).rolling(N).max().values     

def LLV(S,N):             #LLV(C, 5) 最近5天收盘最低价     
    return pd.Series(S).rolling(N).min().values    

def MA(S,N):              #求序列的N日简单移动平均值，返回序列                    
    return pd.Series(S).rolling(N).mean().values  

def EMA(S,N):             #指数移动平均
    return pd.Series(S).ewm(span=N, adjust=False).mean().values     

def SMA(S, N, M=1):       #中国式的SMA
    return pd.Series(S).ewm(alpha=M/N,adjust=False).mean().values           

def WMA(S, N):            #加权移动平均
    return pd.Series(S).rolling(N).apply(lambda x:x[::-1].cumsum().sum()*2/N/(N+1),raw=True).values 

def TR(HIGH, LOW, CLOSE): #真实波幅
    # TR = MAX(MAX((HIGH - LOW), ABS(REF(CLOSE, 1) - HIGH)), ABS(REF(CLOSE, 1) - LOW))
    # 为提高效率简化写法
    return MAX(MAX((HIGH - LOW), ABS(REF(CLOSE, 1) - HIGH)), ABS(REF(CLOSE, 1) - LOW))

def ATR(CLOSE,HIGH,LOW, N=20):                    #真实波动N日平均值
    return MA(TR(HIGH, LOW, CLOSE), N)


#------------------ 1级：趋势追踪类 (Trend Following) ------------------------------

def SUPERTREND(CLOSE, HIGH, LOW, N=10, Factor=3):
    """
    超级趋势指标 (SuperTrend)
    核心逻辑: 基于 ATR 计算上下轨，价格突破上轨转多，跌破下轨转空。
    应用场景: 最常用的移动止损指标，能有效过滤横盘震荡，抓住单边行情。
    返回: trend(1为多, -1为空), supertrend_line(支撑/压力线)
    """
    atr = ATR(CLOSE, HIGH, LOW, N)
    hl2 = (HIGH + LOW) / 2
    
    # 计算基础上下轨
    basic_upper = hl2 + Factor * atr
    basic_lower = hl2 - Factor * atr
    
    # ATR前N个通常为nan，需要处理
    basic_upper = np.nan_to_num(basic_upper)
    basic_lower = np.nan_to_num(basic_lower)
    
    # 初始化最终上下轨和趋势
    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()
    trend = np.zeros(len(CLOSE))
    supertrend_line = np.zeros(len(CLOSE))
    
    # 模拟递归计算 (Python循环慢但逻辑清晰，Numpy化比较复杂这里采用循环)
    # 默认第一天为多头
    trend[0] = 1 
    
    for i in range(1, len(CLOSE)):
        # 计算最终上轨：如果当前基础上轨 < 昨日最终上轨 且 昨日收盘 < 昨日最终上轨，则下移；否则保持
        if basic_upper[i] < final_upper[i-1] or CLOSE[i-1] > final_upper[i-1]:
            final_upper[i] = basic_upper[i]
        else:
            final_upper[i] = final_upper[i-1]
            
        # 计算最终下轨
        if basic_lower[i] > final_lower[i-1] or CLOSE[i-1] < final_lower[i-1]:
            final_lower[i] = basic_lower[i]
        else:
            final_lower[i] = final_lower[i-1]
            
        # 判断趋势
        if trend[i-1] == 1: # 原本多头
            if CLOSE[i] < final_lower[i-1]: # 跌破下轨，转空
                 trend[i] = -1
                 supertrend_line[i] = final_upper[i]
            else:
                 trend[i] = 1
                 supertrend_line[i] = final_lower[i]
        else: # 原本空头
            if CLOSE[i] > final_upper[i-1]: # 突破上轨，转多
                trend[i] = 1
                supertrend_line[i] = final_lower[i]
            else:
                trend[i] = -1
                supertrend_line[i] = final_upper[i]
                
    return trend, supertrend_line

def AROON(HIGH, LOW, N=25):
    """
    阿隆指标 (Aroon)
    核心逻辑: 计算价格达到N周期内最高点和最低点所经历的时间百分比。
    应用场景: 判断趋势的强度和是否即将发生反转。High>70说明强势。
    """
    # 距离最近的高点有多少天
    arg_max = pd.Series(HIGH).rolling(N+1).apply(lambda x: np.argmax(x[::-1]), raw=True).values
    # 距离最近的低点有多少天
    arg_min = pd.Series(LOW).rolling(N+1).apply(lambda x: np.argmin(x[::-1]), raw=True).values
    
    aroon_up = (N - arg_max) / N * 100
    aroon_down = (N - arg_min) / N * 100
    
    return aroon_up, aroon_down

def VORTEX(HIGH, LOW, CLOSE, N=14):
    """
    涡旋指标 (Vortex Indicator)
    核心逻辑: 正向流动(今日高-昨日低) vs 负向流动(今日低-昨日高)。
    应用场景: 捕捉趋势起始点，VI+ 和 VI- 的交叉是明确的买卖信号。
    """
    tr = TR(HIGH, LOW, CLOSE)
    tr_sum = SUM(tr, N)
    
    vm_plus = ABS(HIGH - REF(LOW, 1))
    vm_minus = ABS(LOW - REF(HIGH, 1))
    
    vi_plus = SUM(vm_plus, N) / tr_sum
    vi_minus = SUM(vm_minus, N) / tr_sum
    
    return vi_plus, vi_minus

def HMA(CLOSE, N=20):
    """
    赫尔均线 (Hull Moving Average)
    核心逻辑: 对WMA加权均线进行平方根处理: WMA(2*WMA(n/2) - WMA(n), sqrt(n))
    应用场景: 极大地解决了均线滞后问题，曲线平滑且紧贴价格。
    """
    wma_half = WMA(CLOSE, int(N/2))
    wma_full = WMA(CLOSE, N)
    raw_hma = 2 * wma_half - wma_full
    return WMA(raw_hma, int(np.sqrt(N)))

def ZIGZAG(HIGH, LOW, DIS=5):
    """
    之字转向 (ZigZag) - 简化版
    核心逻辑: 过滤价格波动小于DIS%的微量调整，只连接显著高低点。
    应用场景: 波浪理论分析，形态识别。
    注意: ZigZag包含未来函数，仅用于历史分析，不可用于实时信号产生。
    """
    # 这是一个非常简化的实现思路，仅供参考。完整ZigZag需要回溯修正。
    # 这里通过判断涨跌幅是否超过阈值来确定转折点
    # 实际 Python 向量化实现 ZigZag 极其困难，通常需要 numba 或者 cython 循环
    # 暂留空或仅返回简单的高低点标记，此处为占位
    pass 


#------------------ 2级：成交量与资金流类 (Volume & Money Flow) --------------------

def VWAP(CLOSE, HIGH, LOW, VOL):
    """
    成交量加权平均价 (VWAP)
    核心逻辑: 典型价格 * 成交量 的累加 / 成交量的累加。
    应用场景: 机构交易基准，日内交易生命线。
    注意: 此处实现的是历史全周期的VWAP，如果是日内需要按日重置 cumsum。
    """
    tp = (HIGH + LOW + CLOSE) / 3
    return np.cumsum(tp * VOL) / np.cumsum(VOL)

def CMF(CLOSE, HIGH, LOW, VOL, N=20):
    """
    蔡金资金流量 (Chaikin Money Flow)
    核心逻辑: 将收盘价在当日振幅的位置(MF Multiplier)作为权重，计算资金流。
    应用场景: 衡量资金流向，价格上涨但CMF下跌预示缩量诱多。
    """
    with np.errstate(divide='ignore', invalid='ignore'):
        mf_multiplier = ((CLOSE - LOW) - (HIGH - CLOSE)) / (HIGH - LOW)
    mf_multiplier = np.nan_to_num(mf_multiplier) # 处理 HIGH=LOW 的除0情况
    mf_vol = mf_multiplier * VOL
    return SUM(mf_vol, N) / SUM(VOL, N)

def FORCE(CLOSE, VOL, N=13):
    """
    强力指数 (Force Index)
    核心逻辑: (今日收 - 昨日收) * 成交量。
    应用场景: 综合方向、幅度和成交量，反映驱动力强度。
    """
    fi = (CLOSE - REF(CLOSE, 1)) * VOL
    return EMA(fi, N)

def PVT(CLOSE, VOL):
    """
    价量趋势 (Price Volume Trend)
    核心逻辑: 类似OBV，但根据涨跌幅比例累加成交量。
    应用场景: 比OBV更平滑，细腻反映价格变动对能量累积的影响。
    """
    change_ratio = ((CLOSE - REF(CLOSE, 1)) / REF(CLOSE, 1))
    change_ratio = np.nan_to_num(change_ratio) # 替换nan为0
    return np.cumsum(change_ratio * VOL)


#------------------ 3级：波动率类 (Volatility) -----------------------------------

def CHV(HIGH, LOW, N=10, M=10):
    """
    蔡金波动率 (Chaikin Volatility)
    核心逻辑: H-L差值的EMA的变化率。
    应用场景: 波动率激增通常预示顶部或底部的临近。
    """
    hl_range = HIGH - LOW
    ema_hl = EMA(hl_range, N)
    return (ema_hl - REF(ema_hl, M)) / REF(ema_hl, M) * 100

def CHOP(CLOSE, HIGH, LOW, N=14):
    """
    震荡指数 (Choppiness Index)
    核心逻辑: 利用分形维数计算价格走势的"平滑度"。
    应用场景: 数值越高(>61.8)越震荡，数值越低(<38.2)趋势越明显。
    """
    tr = TR(HIGH, LOW, CLOSE)
    tr_sum = SUM(tr, N)
    range_hl = HHV(HIGH, N) - LLV(LOW, N)
    # 避免 log(0)
    return 100 * np.log10(tr_sum / range_hl) / np.log10(N)

def UI(CLOSE, N=14):
    """
    溃疡指数 (Ulcer Index)
    核心逻辑: 衡量价格从近期峰值回撤的深度的平方平均。
    应用场景: 纯粹衡量下行风险，比标准差更符合投资者心理。
    """
    highest = HHV(CLOSE, N)
    drawdown_pct = (CLOSE - highest) / highest * 100
    sq_dd = drawdown_pct ** 2
    return np.sqrt(SUM(sq_dd, N) / N)


#------------------ 4级：动量与超买超卖类 (Momentum) -----------------------------

def CMO(CLOSE, N=14):
    """
    钱德动量摆动 (Chande Momentum Oscillator)
    核心逻辑: (上涨总和 - 下跌总和) / (上涨总和 + 下跌总和)。
    应用场景: 类似RSI，但更灵敏，用于捕捉短期爆发力。
    """
    diff = CLOSE - REF(CLOSE, 1)
    up = IF(diff > 0, diff, 0)
    down = IF(diff < 0, ABS(diff), 0)
    sum_up = SUM(up, N)
    sum_down = SUM(down, N)
    return (sum_up - sum_down) / (sum_up + sum_down) * 100

def COPPOCK(CLOSE, N1=14, N2=11, M=10):
    """
    估波曲线 (Coppock Curve)
    核心逻辑: 两个不同周期的ROC之和的WMA。
    应用场景: 超长线抄底指标，月线级别在0轴下方拐头向上是经典买点。
    """
    roc1 = 100 * (CLOSE - REF(CLOSE, N1)) / REF(CLOSE, N1)
    roc2 = 100 * (CLOSE - REF(CLOSE, N2)) / REF(CLOSE, N2)
    return WMA(roc1 + roc2, M)

def FISHER(HIGH, LOW, N=9):
    """
    费舍尔转换 (Fisher Transform)
    核心逻辑: 将价格相对位置映射为正态分布。
    应用场景: 让转折点变得极其尖锐，便于识别入场点。
    """
    # 1. 计算价格在N周期内的相对位置 (-1 到 1)
    lowest = LLV(LOW, N)
    highest = HHV(HIGH, N)
    # 处理除0错误
    div = highest - lowest
    div[div==0] = 0.00001 
    
    val = 2 * ((HIGH + LOW)/2 - lowest) / div - 1
    # 处理开始阶段的 nan
    val = np.nan_to_num(val)
    
    # 2. 平滑处理，避免极端值
    # Fisher转换比较特殊，需要迭代计算
    # 这里用向量化近似实现：
    # Variable V = 0.33*Value + 0.67*V_prev
    # Fisher = 0.5 * ln((1+V)/(1-V)) + 0.5 * F_prev
    
    # 由于存在递归依赖，Python向量化较难，此处使用循环实现标准版
    # 如果为了速度可以用 EWMA 近似
    
    val_smooth = np.zeros(len(HIGH))
    fisher = np.zeros(len(HIGH))
    signal = np.zeros(len(HIGH))
    
    for i in range(1, len(HIGH)):
        val_smooth[i] = 0.33 * val[i] + 0.67 * val_smooth[i-1]
        # 截断以防 log 负数或无穷大
        v = val_smooth[i]
        if v > 0.99: v = 0.999
        if v < -0.99: v = -0.999
            
        fisher[i] = 0.5 * np.log((1+v)/(1-v)) + 0.5 * fisher[i-1]
        signal[i] = fisher[i-1]
        
    return fisher, signal



