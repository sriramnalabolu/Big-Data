#!/usr/bin/env python

import sys

for line in sys.stdin:
    list1=[]
    str1=""
    str2=""
    
    line=line.strip()
    
    linewords = line.split(',')
    
    list1.append(linewords[0])
    
    list1.append(linewords[1])
    list1.append(linewords[2])
    list1.append(linewords[3])
    
    for i in range(len(list1)):
        if i>0:
           str2+=list1[i]+','
        else:
           str1+=list1[0]
           
    str2=str2[:-1]
    
    print("%s,%s"%(str1,str2))
        
        
