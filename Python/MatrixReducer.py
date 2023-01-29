#!/usr/bin/env python

import sys

rowAIndex=[]
columnAIndex=[]
rowBIndex=[]
columnBIndex=[]

def index1(input1):
    for line in input1:
        rowAIndex.append(line[0])
        columnAIndex.append(line[1])
        
def index2(input1):
    for line in input1:
        rowBIndex.append(line[0])
        columnBIndex.append(line[1])

mat1=[]
mat2=[]
str1=""

for line in sys.stdin:

    line=line.strip()
    
    list1=line.split(',')
    

    if len(str1) is 0:
       str1+=list1[0]
      
    list2=[]
    for i in range(len(list1)):
        if i>0:
           list2.append(int(list1[i]))
    
    if str1==list1[0]:
       mat1.append(list2)
    else:
       mat2.append(list2)

index1(mat1)
index2(mat2)

rowA=[]
columnB=[]

#inserting all rows from matrix A into a list 
for i in range(max(rowAIndex)+1):   
    rows=[]
    for j in range(max(columnAIndex)+1):
    	for line in mat1:
           if line[0]==i and line[1]==j:
              rows.append(line[2])
    	if len(rows) is 0:
           continue
    	else:
    	   if rows not in rowA:
              rowA.append(rows)
    
#inserting all rows from matrix B into a list
for i in range(max(columnBIndex)+1):
    columns=[]
    for j in range(max(rowBIndex)+1): 
        for line in mat2:
            if line[0]==j and line[1]==i:
               columns.append(line[2])
        if len(columns) is 0:
           continue
        else:
           if columns not in columnB:
              columnB.append(columns)
     
def matMul(Arows,Bcolumns):
    returnList=[] 
    list5=[]
    result=endResult(Arows,Bcolumns)
    
    return result

def endResult(a,b):
    sum1=0
    for i in range(len(a)):
        sum1+=a[i]*b[i]
    return sum1
    
print("Resultant Matrix:")
print("Row,Column,Value")             
for i in range(len(rowA)):
    for j in range(len(columnB)):
        result=""
        result+=str(i)+","+str(j)+","
        res=matMul(rowA[i],columnB[j])
        
        result+=str(res)
        
        print(result)
        
    
        
        
        

