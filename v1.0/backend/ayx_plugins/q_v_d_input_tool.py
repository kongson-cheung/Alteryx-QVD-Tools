# Copyright (C) 2022 Alteryx, Inc. All rights reserved.
#
# Licensed under the ALTERYX SDK AND API LICENSE AGREEMENT;
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.alteryx.com/alteryx-sdk-and-api-license-agreement
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from collections import namedtuple

from ayx_python_sdk.core import PluginV2
from ayx_python_sdk.providers.amp_provider.amp_provider_v2 import AMPProviderV2

from pyarrow import Table


import xml.etree.ElementTree as ET
from dataclasses import dataclass
from enum import Enum, auto
from io import BytesIO
import struct
from datetime import date, datetime
import sys


class QVDInputTool(PluginV2):
    """Concrete implementation of an AyxPlugin."""

    def __init__(self, provider: AMPProviderV2) -> None:
        """Construct a plugin."""
        self.provider = provider
        self.config_value = 0.42
        self.provider.io.info("QVD Input Tool initialized.")

    def on_incoming_connection_complete(self, anchor: namedtuple) -> None:
        """
        Call when an incoming connection is done sending data including when no data is sent on an optional input anchor.

        This method IS NOT called during update-only mode.

        Parameters
        ----------
        anchor
            NamedTuple containing anchor.name and anchor.connection.
        """
        raise NotImplementedError("Input tools don't receive batches.")

    def on_record_batch(self, batch: "Table", anchor: namedtuple) -> None:
        """
        Process the passed record batch that comes in on the specified anchor.

        The method that gets called whenever the plugin receives a record batch on an input.

        This method IS NOT called during update-only mode.

        Parameters
        ----------
        batch
            A pyarrow Table containing the received batch.
        anchor
            A namedtuple('Anchor', ['name', 'connection_name']) containing input connection identifiers.
        """
        raise NotImplementedError("Input tools don't receive batches.")

    def on_complete(self) -> None:
        """
        Clean up any plugin resources, or push records for an input tool.

        This method gets called when all other plugin processing is complete.

        In this method, a Plugin designer should perform any cleanup for their plugin.
        However, if the plugin is an input-type tool (it has no incoming connections),
        processing (record generation) should occur here.

        Note: A tool with an optional input anchor and no incoming connections should
        also write any records to output anchors here.
        """
        import pandas as pd
        import pyarrow as pa
        
        QVDFile = self.provider.tool_config["QVDFile"]
        
        self.provider.io.info("QVDInputTool starts reading from " + QVDFile)
        
        qvdConverter = QVDConverter(QVDFile)
        
        self.provider.write_to_anchor("Output", pa.Table.from_pandas(qvdConverter.ReadAllRecords(self.provider.io), preserve_index=False))
        
        self.provider.io.info("QVDInputTool finished reading from " + QVDFile)
        
        qvdConverter = None







# Enum for FieldTag.Value
class Value(Enum):
    NUMERIC = '$numeric'
    INTEGER = '$integer'
    ASCII = '$ascii'
    TEXT = '$text'
    TIMESTAMP = '$timestamp'
    DATE = '$date'
    HIDDEN = '$hidden'
    KEY = '$key'
    

# Enum for FieldAttrType.Type
class FieldType(Enum):
    UNKNOWN = 'UNKNOWN'
    ASCII = 'ASCII'
    DATE = 'DATE'
    TIMESTAMP = 'TIMESTAMP'
    INTEGER = 'INTEGER'
    REAL = 'REAL'
    INTERVAL = 'INTERVAL'
    FIX = 'FIX'


@dataclass
class NumberFormat:
    Type: FieldType = FieldType.UNKNOWN
    nDec: int=0
    UseThou: int=0
    Fmt: str=None
    Dec: str=None
    Thou: str=None

@dataclass
class Tags:
    String: [] =None

@dataclass
class LineageInfo:
    Discriminator: str=""
    Statement: str=""

@dataclass
class Lineage:
    LineageInfo: LineageInfo=None
    
@dataclass
class Fields:
    QvdFieldHeader: []=None
    
@dataclass
class QvdTableHeader:
    QvBuildNo: int=0
    CreatorDoc: str=""
    CreateUtcTime: str=""
    SourceCreateUtcTime: str=""
    SourceFileUtcTime: str=""
    StaleUtcTime: str=""
    TableName: str=""
    SourceFileSize: int=0
    Fields: Fields = None
    Compression: str=""
    RecordByteSize: int=0
    NoOfRecords: int =0
    Offset: int=0
    Length: int =0
    Comment: str=""
    Lineage: Lineage = None


@dataclass
class QvdFieldHeader:
    FieldName: str=""
    BitOffset: int=0
    BitWidth: int=0
    Bias: int=0
    NumberFormat: NumberFormat=None
    NoOfSymbols: int=0
    Offset: int=0
    Length: int=0
    Comment: str=""
    Tags: Tags=None
#    _SymbolText : [] = None
#    _SymbolInt : [] = None
    _SymbolVal : [] = None
    _SymbolBytes: [] = None
  

class QVDXMLParser:

    def GetQvdTableHeader(self, XMLContent):    
        root = ET.fromstring(XMLContent)    
        qvdTableHeader = QvdTableHeader()
        
        qvdTableHeader.QvBuildNo=int(root.find("QvBuildNo").text)
        qvdTableHeader.CreatorDoc=root.find("CreatorDoc").text
        qvdTableHeader.CreateUtcTime=root.find("CreateUtcTime").text
        qvdTableHeader.SourceCreateUtcTime=root.find("SourceCreateUtcTime").text
        qvdTableHeader.SourceFileUtcTime=root.find("SourceFileUtcTime").text
        qvdTableHeader.StaleUtcTime=root.find("StaleUtcTime").text
        qvdTableHeader.TableName=root.find("TableName").text
        qvdTableHeader.SourceFileSize=int(root.find("SourceFileSize").text)
        qvdTableHeader.Compression=root.find("Compression").text
        qvdTableHeader.RecordByteSize=int(root.find("RecordByteSize").text)
        qvdTableHeader.NoOfRecords=int(root.find("NoOfRecords").text)
        qvdTableHeader.Offset=int(root.find("Offset").text)
        qvdTableHeader.Length=int(root.find("Length").text)
        qvdTableHeader.Comment=root.find("Comment").text
        
        if root.find("Lineage") is not None:
            qvdTableHeader.Lineage = Lineage()
            
            if root.find("Lineage").find('LineageInfo') is not None:
                qvdTableHeader.Lineage.LineageInfo = LineageInfo()
                              
                if root.find("Lineage").find('LineageInfo').find("Discriminator") is not None:
                    qvdTableHeader.Lineage.LineageInfo.Discriminator = root.find("Lineage").find('LineageInfo').find('Discriminator').text

                if root.find("Lineage").find('LineageInfo').find('Statement') is not None:
                    qvdTableHeader.Lineage.LineageInfo.Statement  =root.find("Lineage").find('LineageInfo').find("Statement").text


        qvdTableHeader.Fields = Fields()
        qvdTableHeader.Fields.QvdFieldHeader = []
        fields = root.find("Fields")
        for qvdTableFieldHeader in fields.iter("QvdFieldHeader"):
            qvdFieldHeader  = QvdFieldHeader()
            qvdTableHeader.Fields.QvdFieldHeader.append(qvdFieldHeader)
            
            
            qvdFieldHeader.FieldName = qvdTableFieldHeader.find("FieldName").text
            qvdFieldHeader.BitOffset = int(qvdTableFieldHeader.find("BitOffset").text)
            qvdFieldHeader.BitWidth = int(qvdTableFieldHeader.find("BitWidth").text)
            qvdFieldHeader.Bias = int(qvdTableFieldHeader.find("Bias").text)
            qvdFieldHeader.NoOfSymbols = int(qvdTableFieldHeader.find("NoOfSymbols").text)
            qvdFieldHeader.Offset = int(qvdTableFieldHeader.find("Offset").text)
            qvdFieldHeader.Length = int(qvdTableFieldHeader.find("Length").text)
            qvdFieldHeader.Comment = qvdTableFieldHeader.find("Comment").text

            numberFormat = NumberFormat()
            qvdFieldHeader.NumberFormat = NumberFormat
            numberFormat.Type = FieldType(qvdTableFieldHeader.find("NumberFormat").find("Type").text)
            numberFormat.nDec = int(qvdTableFieldHeader.find("NumberFormat").find("nDec").text)
            numberFormat.UseThou = int(qvdTableFieldHeader.find("NumberFormat").find("UseThou").text)
            numberFormat.Fmt = qvdTableFieldHeader.find("NumberFormat").find("Fmt").text
            numberFormat.Dec = qvdTableFieldHeader.find("NumberFormat").find("Dec").text
            numberFormat.Thou = qvdTableFieldHeader.find("NumberFormat").find("Thou").text
  
            tags = Tags()
            tags.String = []
            qvdFieldHeader.Tags = tags
            tagsXML = qvdTableFieldHeader.find("Tags")
            for t in tagsXML.iter("String"):
                tags.String.append(Value(t.text))   

        return qvdTableHeader
   

class QVDConverter:
    bitMask = [
        0, 1, 3, 7, 15, 31, 63, 127, 255, 511, 1023, 2047, 4095, 8191, 16383, 32767,
        65535, 131071, 262143, 524287, 1048575, 2097151, 4194303, 8388607, 16777215,
        33554431, 67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647,
        4294967295, 8589934591, 17179869183, 34359738367, 68719476735, 137438953471,
        274877906943, 549755813887, 1099511627775, 2199023255551, 4398046511103,
        8796093022207, 17592186044415, 35184372088831, 70368744177663, 140737488355327,
        281474976710655, 562949953421311, 1125899906842623, 2251799813685247,
        4503599627370495, 9007199254740991, 18014398509481983, 36028797018963967,
        72057594037927935, 144115188075855871, 288230376151711743, 576460752303423487,
        1152921504606846975, 2305843009213693951, 4611686018427387903,
        9223372036854775807, 18446744073709551615
    ]
    
    qvdTableHeader : QvdTableHeader = None

    allRecordBytes: bytes()
    qvdFile : str
    columns : []

   
    def __init__(self, fileName):
        self.qvdFile = fileName
        self.qvdTableHeader = QvdTableHeader()
        
        self.ReadQVD(fileName)        
    
    def ReadQVD(self, fileName):
    
        allBytes = bytes()
        qvdXMLParser = QVDXMLParser()

        
        with open(fileName, 'rb') as file:
            allBytes = file.read()

        #Read XML and the separator NULL
        xmlEndPosition = allBytes.find(b'\x00')
        
        XMLContent = allBytes[:xmlEndPosition].decode('utf-8')
        self.qvdTableHeader = qvdXMLParser.GetQvdTableHeader(XMLContent)
        
        
        #process Symbol
        for qvdFieldHeader in self.qvdTableHeader.Fields.QvdFieldHeader:
            startPos = xmlEndPosition + qvdFieldHeader.Offset +1
            endPos = startPos + qvdFieldHeader.Length
            
            qvdFieldHeader._SymbolBytes = allBytes[startPos:endPos]
            qvdFieldHeader._SymbolVal = [None] * qvdFieldHeader.NoOfSymbols
            #qvdFieldHeader._SymbolInt = [None] * qvdFieldHeader.NoOfSymbols
            #qvdFieldHeader._SymbolText = [None] * qvdFieldHeader.NoOfSymbols

        self.columns = [None] * len(self.qvdTableHeader.Fields.QvdFieldHeader)
        
        #process Record
        startPos = xmlEndPosition + self.qvdTableHeader.Offset +1
        endPos = startPos + self.qvdTableHeader.Length
        self.allRecordBytes = allBytes[startPos:endPos]
        
        #free memory
        allBytes = None

       
        #currently single thread
        self.ReadAllSymbol()
   
        
    def ReadAllSymbol(self):
        for j in range(len(self.qvdTableHeader.Fields.QvdFieldHeader)):
            self.ReadSymbol(j)
            self.columns[j] = self.qvdTableHeader.Fields.QvdFieldHeader[j].FieldName
            
            self.qvdTableHeader.Fields.QvdFieldHeader[j]._SymbolBytes = None


    def ReadSymbol(self, fieldIndex):
        qvdFieldHeader = self.qvdTableHeader.Fields.QvdFieldHeader[fieldIndex]
        startPos = 0
        readPos = 0
        endPos = len(qvdFieldHeader._SymbolBytes)
        
        for j in range(qvdFieldHeader.NoOfSymbols):
            
            if readPos <=endPos:
                symbolType = qvdFieldHeader._SymbolBytes[readPos]
                readPos += 1
                startPos = readPos
                
                if symbolType == 1:
                    readPos += 4
                    #qvdFieldHeader._SymbolInt[j] = struct.unpack('<i', qvdFieldHeader._SymbolBytes[startPos:readPos])[0]
                    qvdFieldHeader._SymbolVal[j] = struct.unpack('<i', qvdFieldHeader._SymbolBytes[startPos:readPos])[0]

                elif symbolType == 2:
                    readPos += 8          
                    #qvdFieldHeader._SymbolInt[j] = struct.unpack('<d', qvdFieldHeader._SymbolBytes[startPos:readPos])[0]
                    qvdFieldHeader._SymbolVal[j] = struct.unpack('<d', qvdFieldHeader._SymbolBytes[startPos:readPos])[0]


                elif symbolType == 3:
                    pass
                
                elif symbolType == 4:
                    while qvdFieldHeader._SymbolBytes[readPos] > 0:
                        readPos += 1

                    #qvdFieldHeader._SymbolText[j] = qvdFieldHeader._SymbolBytes[startPos:readPos].decode("utf-8")
                    qvdFieldHeader._SymbolVal[j] = qvdFieldHeader._SymbolBytes[startPos:readPos].decode("utf-8")
                    
                    readPos += 1
                elif symbolType == 5:
                    readPos += 4
                    
                    #qvdFieldHeader._SymbolInt[j] = struct.unpack('<i', qvdFieldHeader._SymbolBytes[startPos:readPos])[0]
                   
                    startPos = readPos
                    while qvdFieldHeader._SymbolBytes[readPos] > 0:
                        readPos += 1
                    
                    #qvdFieldHeader._SymbolText[j] = qvdFieldHeader._SymbolBytes[startPos:readPos].decode("utf-8")
                    qvdFieldHeader._SymbolVal[j] = qvdFieldHeader._SymbolBytes[startPos:readPos].decode("utf-8")
                    
                    readPos += 1
                    
                elif symbolType == 6:
                    readPos += 8
                    
                    #qvdFieldHeader._SymbolInt[j] = struct.unpack('<d', qvdFieldHeader._SymbolBytes[startPos:readPos])[0]
                
                    startPos = readPos
                    while qvdFieldHeader._SymbolBytes[readPos] > 0:
                        readPos += 1
                    
                    #qvdFieldHeader._SymbolText[j] = qvdFieldHeader._SymbolBytes[startPos:readPos].decode("utf-8")
                    qvdFieldHeader._SymbolVal[j] = qvdFieldHeader._SymbolBytes[startPos:readPos].decode("utf-8")
                    
                    readPos+= 1


    def ReadAllRecords(self, io):

        io.info("Total number of records: " + str(self.qvdTableHeader.NoOfRecords))
        
        import pandas as pd
        
        data = [None] * self.qvdTableHeader.NoOfRecords

        for r in range(self.qvdTableHeader.NoOfRecords):
            data[r] = self.ReadRecord(r)

            if  (r+1) % 1000000 == 0:
                io.info("Read " + str(r+1) + " records ...")

        df = pd.DataFrame(data, columns=self.columns)
        data = None

        return df 


    def ReadRecord(self, recordIndex):
        startPos = recordIndex * self.qvdTableHeader.RecordByteSize
        endPos = startPos + self.qvdTableHeader.RecordByteSize 
        result = int.from_bytes(self.allRecordBytes[startPos:endPos], byteorder='little', signed=False)
        
        record = [None] * len(self.qvdTableHeader.Fields.QvdFieldHeader)
        
        for j in range(len(self.qvdTableHeader.Fields.QvdFieldHeader)):
            qvdFieldHeader = self.qvdTableHeader.Fields.QvdFieldHeader[j]
                       
            if qvdFieldHeader.Bias==0:
                index = (result >> qvdFieldHeader.BitOffset) & self.bitMask[qvdFieldHeader.BitWidth] 
                
                record[j] = qvdFieldHeader._SymbolVal[index]
                
                
        return record   
