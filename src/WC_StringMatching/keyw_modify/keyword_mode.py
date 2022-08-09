# -*- coding:utf-8 -*-

'''
python ver. 3.6.12

Collection of functions which modify keyword strings 

'''
# import numpy as np
# import pickle
import re


print(f'{"="*80}')
print(f"imported : {__file__}")
print(f"namespace : {__name__}")

#---------------------------------------------------------- function declaration
def uniform_match(aKeyword) :
    keywLength = len(aKeyword)
    if keywLength > 4:
        unifiedKeyw = aKeyword.lower()
    elif keywLength > 2:
        unifiedKeyw = aKeyword[0].upper()+aKeyword[1:]
    else :
        unifiedKeyw = aKeyword
    return unifiedKeyw

def uniform_patt(aKeyword) :
    keywLength = len(aKeyword)
    if keywLength > 4:
        pattString = "(?i)\\b%s\\b" %re.escape(aKeyword)
    elif keywLength > 2:
        pattString = "\\b(?i:%s)%s\\b" %(aKeyword[0], aKeyword[1:])
    else :
        pattString = "\\b%s\\b" %re.escape(aKeyword)
    outPattern = re.compile(pattString)
    return outPattern

def uniform_keyw(aKeyword) :
    #outputKeyw = keywordList[aKeyword][0]
    keywLength = len(aKeyword)
    outputKeyw = aKeyword
    outputKeyw = re.sub(' ?, ?', ',', outputKeyw)
    outputKeyw = re.sub('\[', '(', outputKeyw)
    outputKeyw = re.sub('\]', ')', outputKeyw)
    outputKeyw = re.sub('(?i)^probable ', '', outputKeyw)
    outputKeyw = re.sub('(?i)^putative ', '', outputKeyw)
    outputKeyw = re.sub('(?i) homolog(,? \w+)?$', '', outputKeyw)
    outputKeyw = re.sub('(?i) isoform(,? \w+)?$', '', outputKeyw)
    if keywLength > 4 :
        outputKeyw = outputKeyw.lower()
    elif keywLength > 2 :
        outputKeyw = outputKeyw.title()
    return outputKeyw

def uniform_keyw_2(aKeyword) :
    #outputKeyw = keywordList[aKeyword][0]
    keywLength = len(aKeyword)
    outputKeyw = aKeyword
    outputKeyw = re.sub(' ?, ?', ',', outputKeyw)
    outputKeyw = re.sub('\[', '(', outputKeyw)
    outputKeyw = re.sub('\]', ')', outputKeyw)
    outputKeyw = re.sub('(?i)^probable ', '', outputKeyw)
    outputKeyw = re.sub('(?i)^putative ', '', outputKeyw)
    outputKeyw = re.sub('(?i) homolog(,? \w+)?$', '', outputKeyw)
    outputKeyw = re.sub('(?i) isoform(,? \w+)?$', '', outputKeyw)
    if keywLength > 4 :
        outputKeyw = outputKeyw.lower()
    elif keywLength > 2 :
        outputKeyw = outputKeyw[0].upper()+outputKeyw[1:]
    return outputKeyw

def get_ngram_list2(fullPhrase) :
    delimitIteration = re.finditer('''([]!@#$%*()\\-=\\+{}|\\\:;<,>\\.?/~`'"[])|\s+''', fullPhrase)
    gramStart = 0
    phraseList = []
    gramStartList = []
    for occurIdx, delimOccur in enumerate(delimitIteration) :
        gramIdx = occurIdx*2 
        gramEnd = delimOccur.start()
        phraseList.append( fullPhrase[gramStart:gramEnd])
        gramStartList.append( gramStart )
        gramStart = delimOccur.end()
        phraseList.append( fullPhrase[gramEnd:gramStart] )
        gramStartList.append(  gramEnd )
    phraseList.append( fullPhrase[gramStart:] )
    gramStartList.append( gramStart )
    phraseIdx = 0
    #debug_log = []
    while phraseIdx < len(phraseList) :
        phraseToSplit = phraseList[phraseIdx].strip()
        if phraseToSplit == '' :  
            del phraseList[phraseIdx]
            del gramStartList[phraseIdx]
            #debug_log.append(1)
            continue
        if phraseToSplit == None :
            del phraseList[phraseIdx]
            del gramStartList[phraseIdx]
            #debug_log.append(2)
            continue
        if '$' == phraseToSplit :
            if re.fullmatch('[0-9]+', phraseList[phraseIdx+1]):
                phraseList[phraseIdx:phraseIdx+2] = [ '$'+phraseList[phraseIdx+1] ]
                gramStartList[phraseIdx:phraseIdx+2] = [ gramStartList[phraseIdx] ]
                phraseIdx += 1
                #debug_log.append(3)
                continue
        if '.' == phraseToSplit :
            if phraseIdx == 0 : 
                phraseIdx += 1
                #debug_log.append(4)
                continue
            elif re.fullmatch('\\$?[0-9]+', phraseList[phraseIdx-1]) and re.fullmatch('[0-9]+', phraseList[phraseIdx+1]):
                phraseList[phraseIdx-1:phraseIdx+2] = [ phraseList[phraseIdx-1]+'.'+phraseList[phraseIdx+1] ]
                gramStartList[phraseIdx-1:phraseIdx+2] = [ gramStartList[phraseIdx-1] ]
                #debug_log.append(5)
                continue
        if '+' == phraseToSplit :
            if phraseIdx == 0 : 
                phraseIdx += 1
                #debug_log.append(6)
                continue
            elif re.fullmatch('[a-zA-Z0-9]+\\+*', phraseList[phraseIdx-1]):
                phraseList[phraseIdx-1:phraseIdx+1] = [ phraseList[phraseIdx-1]+'+' ]
                gramStartList[phraseIdx-1:phraseIdx+1] = [ gramStartList[phraseIdx-1] ]
                #debug_log.append(7)
                continue
        #debug_log.append(8)
        phraseIdx += 1
    
    return (phraseList, gramStartList)

def plural_keyw(text=''):
    postfix = 's'
    if len(text) > 2:
        if text[-2:] in ('ch', 'sh', 'ss'):
            postfix = 'es'
        elif text[-1:] in ('s', 'x', 'z'):
            postfix = 'es'
    result = '%s%s' % (text, postfix)
    return result    


def get_ngram_list3(fullPhrase) :
    delimitIteration = re.finditer(
        '''(?a)([]!@#$%*()\\-=\\+{}|\\\:;<,>\\.?/~`'"[])|\W|\d+''', fullPhrase)
    gramStart = 0
    phraseList = []
    gramStartList = []
    
    for occurIdx, delimOccur in enumerate(delimitIteration) :
        gramIdx = occurIdx*2 
        gramEnd = delimOccur.start()
        phraseList.append( fullPhrase[gramStart:gramEnd])
        gramStartList.append( gramStart )
        gramStart = delimOccur.end()
        phraseList.append( fullPhrase[gramEnd:gramStart] )
        gramStartList.append(  gramEnd )
    phraseList.append( fullPhrase[gramStart:] )
    gramStartList.append( gramStart )
    phraseIdx = 0
    #debug_log = []
    
    while phraseIdx < len(phraseList) :
        phraseToSplit = phraseList[phraseIdx].strip()
        if phraseToSplit == '' :  
            del phraseList[phraseIdx]
            del gramStartList[phraseIdx]
            #debug_log.append(1)
            continue
        if phraseToSplit == None :
            del phraseList[phraseIdx]
            del gramStartList[phraseIdx]
            #debug_log.append(2)
            continue
        if '$' == phraseToSplit :
            if re.fullmatch('[0-9]+', phraseList[phraseIdx+1]):
                phraseList[phraseIdx:phraseIdx+2] = [ '$'+phraseList[phraseIdx+1] ]
                gramStartList[phraseIdx:phraseIdx+2] = [ gramStartList[phraseIdx] ]
                phraseIdx += 1
                #debug_log.append(3)
                continue
        if '.' == phraseToSplit :
            if phraseIdx == 0 : 
                phraseIdx += 1
                #debug_log.append(4)
                continue
            elif re.fullmatch('\\$?[0-9]+', phraseList[phraseIdx-1]) and re.fullmatch('[0-9]+', phraseList[phraseIdx+1]):
                phraseList[phraseIdx-1:phraseIdx+2] = [ phraseList[phraseIdx-1]+'.'+phraseList[phraseIdx+1] ]
                gramStartList[phraseIdx-1:phraseIdx+2] = [ gramStartList[phraseIdx-1] ]
                #debug_log.append(5)
                continue
        if '+' == phraseToSplit :
            if phraseIdx == 0 : 
                phraseIdx += 1
                #debug_log.append(6)
                continue
            elif re.fullmatch('[a-zA-Z0-9]+\\+*', phraseList[phraseIdx-1]):
                phraseList[phraseIdx-1:phraseIdx+1] = [ phraseList[phraseIdx-1]+'+' ]
                gramStartList[phraseIdx-1:phraseIdx+1] = [ gramStartList[phraseIdx-1] ]
                #debug_log.append(7)
                continue
        #debug_log.append(8)
        phraseIdx += 1
    
    return (phraseList, gramStartList)

greek_dict = {
'α'     :    "alpha",       #'\u03B1'
'β'     :    "beta",        #'\u03B2'
'γ'     :    "gamma",       #'\u03B3'
'δ'     :    "delta",       #'\u03B4'
'ε'     :    "epsilon",     #'\u03B5'
'ζ'     :    "zeta",        #'\u03B6'
'η'     :    "eta",     #'\u03B7'
'θ'     :    "theta",       #'\u03B8'
'ι'     :    "iota",        #'\u03B9'
'κ'     :    "kappa",       #'\u03BA'
'λ'     :    "lamda",       #'\u03BB'
'μ'     :    "mu",      #'\u03BC'
'ν'     :    "nu",      #'\u03BD'
'ξ'     :    "xi",      #'\u03BE'
'ο'     :    "omicron",     #'\u03BF'
'π'     :    "pi",      #'\u03C0'
'ρ'     :    "rho",     #'\u03C1'
'ς'     :    "final",       #'\u03C2' SIGMA
'σ'     :    "sigma",       #'\u03C3'
'τ'     :    "tau",     #'\u03C4'
'υ'     :    "upsilon",     #'\u03C5'
'φ'     :    "phi",     #'\u03C6'
'χ'     :    "chi",     #'\u03C7'
'ψ'     :    "psi",     #'\u03C8'
'ω'     :    "omega",       #'\u03C9'

'Α'     :    "alpha",       #'\u0391'
'Β'     :    "beta",        #'\u0392'
'Γ'     :    "gamma",       #'\u0393'
'Δ'     :    "delta",       #'\u0394'
'Ε'     :    "epsilon",     #'\u0395'
'Ζ'     :    "zeta",        #'\u0396'
'Η'     :    "eta",     #'\u0397'
'Θ'     :    "theta",       #'\u0398'
'Ι'     :    "iota",        #'\u0399'
'Κ'     :    "kappa",       #'\u039A'
'Λ'     :    "lamda",       #'\u039B'
'Μ'     :    "mu",      #'\u039C'
'Ν'     :    "nu",      #'\u039D'
'Ξ'     :    "xi",      #'\u039E'
'Ο'     :    "omicron",     #'\u039F'
'Π'     :    "pi",      #'\u03A0'
'Ρ'     :    "rho",     #'\u03A1'
'Σ'     :    "sigma",       #'\u03A3'
'Τ'     :    "tau",     #'\u03A4'
'Υ'     :    "upsilon",     #'\u03A5'
'Φ'     :    "phi",     #'\u03A6'
'Χ'     :    "chi",     #'\u03A7'
'Ψ'     :    "psi",     #'\u03A8'
'Ω'     :    "omega",       #'\u03A9'
'ϴ'     :    "theta"}       #'\u03F4' SYMBOL