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

from ayx_python_sdk.core import (
    Anchor,
    PluginV2,
)
from ayx_python_sdk.providers.amp_provider.amp_provider_v2 import AMPProviderV2

import pyarrow as pa


import xml.etree.ElementTree as ET
from dataclasses import dataclass
from enum import Enum, auto
from io import BytesIO
import struct
from datetime import date, datetime
import sys
import time
import traceback


class QVDOutputTool(PluginV2):
    qvdConverter : None
    batchTable : None

    def __init__(self, provider: AMPProviderV2):
        """Construct a plugin."""
        self.provider = provider
        self.provider.io.info("QVD Output Tool initialized")
        
        QVDFile = self.provider.tool_config["QVDFile"]
        self.qvdConverter = QVDConverter(QVDFile)
        self.batchTable = None

    def on_record_batch(self, batch: "pa.Table", anchor: Anchor) -> None:
        """
        Process the passed record batch.

        The method that gets called whenever the plugin receives a record batch on an input.

        This method IS NOT called during update-only mode.

        Parameters
        ----------
        batch
            A pyarrow Table containing the received batch.
        anchor
            A namedtuple('Anchor', ['name', 'connection']) containing input connection identifiers.
        """
        if self.batchTable is None:
            self.batchTable = batch
        else:
            self.batchTable = pa.concat_tables([self.batchTable, batch])
                
        
    def on_incoming_connection_complete(self, anchor: Anchor) -> None:
        """
        Call when an incoming connection is done sending data including when no data is sent on an optional input anchor.

        This method IS NOT called during update-only mode.

        Parameters
        ----------
        anchor
            NamedTuple containing anchor.name and anchor.connection.
        """

        

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
        self.qvdConverter.WriteRecords(self.batchTable, self.provider.io)
        self.provider.io.info("Finished QVD Processing and writing to file...")

        self.qvdConverter.WriteQVD()
        self.provider.io.info("QVD Output Tool finished writing to " + self.provider.tool_config["QVDFile"])


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
    _Symbol : {} = None
    _SymbolBytes : bytearray() = None
  

class QVDXMLParser:

     def WriteQVDXML(self, qvdTableHeader, file):
        root = ET.Element('QvdTableHeader')

        qvdTableHeaderElements = ['QvBuildNo', 'CreatorDoc', 'CreateUtcTime', 'SourceCreateUtcTime',     
                                'SourceFileUtcTime', 'SourceFileSize', 'StaleUtcTime', 'TableName']
        
        for element in qvdTableHeaderElements:
            child = ET.Element(element)
            child.text = str(getattr(qvdTableHeader, element))
            child.text = child.text if child.text != "None" else ""
            root.append(child)
            
        fieldsElement = ET.Element('Fields')
        root.append(fieldsElement)
        
        for qvdfieldHeader in qvdTableHeader.Fields.QvdFieldHeader:
            fieldHeaderElement = ET.SubElement(fieldsElement, "QvdFieldHeader")
            
            
            qvdFieldHeaderElements = ['FieldName', 'BitOffset', 'BitWidth', 'Bias']

            for element in qvdFieldHeaderElements:
                child = ET.SubElement(fieldHeaderElement, element)
                child.text = str(getattr(qvdfieldHeader, element))  
                child.text = child.text if child.text != "None" else ""

            
            numberFormatElement = ET.SubElement(fieldHeaderElement, 'NumberFormat')
            numberFormatChildElements = ['Type', 'nDec','UseThou','Fmt','Dec', 'Thou']
            for element in numberFormatChildElements:
                child = ET.SubElement(numberFormatElement, element)
                if element == "Type":
                    child.text = str(getattr(qvdfieldHeader.NumberFormat, element).value)
                    child.text = child.text if child.text != "None" else ""
                else:
                    child.text = str(getattr(qvdfieldHeader.NumberFormat, element))
                    child.text = child.text if child.text != "None" else ""

            qvdFieldHeaderElements = ['NoOfSymbols', 'Offset', 'Length', 'Comment']

            for element in qvdFieldHeaderElements:
                child = ET.SubElement(fieldHeaderElement, element)
                child.text = str(getattr(qvdfieldHeader, element))  
                child.text = child.text if child.text != "None" else ""


            tagsElement = ET.SubElement(fieldHeaderElement, 'Tags')
            for string in qvdfieldHeader.Tags.String:
                child = ET.SubElement(tagsElement, 'String')
                child.text = string

        qvdTableHeaderElements = ['Compression', 'RecordByteSize', 'NoOfRecords', 'Offset',     
                                'Length']
        for element in qvdTableHeaderElements:
            child = ET.SubElement(root, element)
            child.text = str(getattr(qvdTableHeader, element))
            child.text = child.text if child.text != "None" else ""

        lineageElement = ET.SubElement(root, 'Lineage')
        lineageInfoElement = ET.SubElement(lineageElement, 'LineageInfo')
        
        discriminatorElement = ET.SubElement(lineageInfoElement, 'Discriminator')
        discriminatorElement.text = str(qvdTableHeader.Lineage.LineageInfo.Discriminator)
        discriminatorElement.text = discriminatorElement.text if discriminatorElement.text != "None" else ""
            
        statementElement = ET.SubElement(lineageInfoElement, 'Statement')
        statementElement.text = str(qvdTableHeader.Lineage.LineageInfo.Statement)
        statementElement.text = statementElement.text if statementElement.text != "None" else ""
        
        child = ET.SubElement(root, 'Comment')
        child.text = str(getattr(qvdTableHeader, 'Comment'))
        child.text = child.text if child.text != "None" else ""
        
        
        tree = ET.ElementTree(root)
        
        #need python 3.9?
        #ET.indent(tree, space='   ')
        
        tree.write(file, encoding="utf-8", xml_declaration=True, short_empty_elements=False, method='xml')
        
   

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
    qvdFile : str
    recordBytes: None


    def __init__(self, fileName):
        self.qvdFile = fileName
        self.qvdTableHeader = QvdTableHeader()

        self.qvdTableHeader.Fields = []
        self.qvdTableHeader.QvBuildNo = "50668"
        self.qvdTableHeader.CreatorDoc = fileName
        self.qvdTableHeader.CreateUtcTime = time.strftime("%Y-%m-%d %H:%M:%S")
        self.qvdTableHeader.SourceCreateUtcTime = None
        self.qvdTableHeader.SourceFileUtcTime = None
        self.qvdTableHeader.SourceFileSize = -1
        self.qvdTableHeader.StaleUtcTime = None
        self.qvdTableHeader.TableName = 'ALTERYX_QVD_OUTPUT_TOOL'
        self.qvdTableHeader.Fields = Fields()
        self.qvdTableHeader.Fields.QvdFieldHeader = []
        self.qvdTableHeader.Lineage = Lineage()
        self.qvdTableHeader.Lineage.LineageInfo = LineageInfo()

        

    def WriteRecords(self, batch, io ):
        import pyarrow as pa      
        
        """
        ====Alteryx Data Type and pyarrow data type====
        blob, column.type: binary
        bool, column.type: bool
        byte, column.type: uint8
        int16, column.type: int16
        int32, column.type: int32
        int64, column.type: int64
        fixdecimal, column.type: decimal256(3, 2)
        float, column.type: float
        double, column.type: double
        string, column.type: string
        WString, column.type: string
        v_string, column.type: string
        V_WString, column.type: string
        date, column.type: date32[day]
        Time, column.type: time32[s]
        datetime, column.type: timestamp[s]
        =================================================
        """
        
        io.info("Total number of records: " + str(len(batch)))

        
        intTypes = ['bool', 'uint8','int16', 'int32', 'int64']
        floatingPointTypes = ['float', 'double']
        dateTypes = ['date32[day]']
        dateTimeTypes = ['time32[s]', 'timestamp[s]']
        stringType = 'string'
        
        self.qvdTableHeader.NoOfRecords = len(batch)

        offset = 0
        bitOffset = 0

        #process each field
        for colIndex in range(len(batch.schema)):
            
            column = batch.column(colIndex)
            
            qvdFieldHeader = QvdFieldHeader()
            
            qvdFieldHeader.FieldName = batch.schema.names[colIndex]
                            
            qvdFieldHeader.NumberFormat = NumberFormat()
                                            
            qvdFieldHeader.Tags = Tags()
            qvdFieldHeader.Tags.String = []

            qvdFieldHeader._SymbolBytes = bytearray()
            qvdFieldHeader._Symbol  = {}
              
            self.qvdTableHeader.Fields.QvdFieldHeader.append(qvdFieldHeader)                   
            

            columnType = str(column.type)
            symbolType = -1
            
            #classifying symbol type
            if columnType in dateTimeTypes:                       
                if columnType == 'time32[s]':
                    symbolType = 6
                elif columnType == 'timestamp[s]':
                    symbolType = 66
                 
                qvdFieldHeader.NumberFormat.Type = FieldType.TIMESTAMP
                        
                if Value.NUMERIC.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.NUMERIC.value)
                if Value.TIMESTAMP.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.TIMESTAMP.value)
                        
            elif columnType in dateTypes:                       
                symbolType = 5

                qvdFieldHeader.NumberFormat.Type = FieldType.DATE
                        
                if Value.INTEGER.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.INTEGER.value)
                if Value.NUMERIC.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.NUMERIC.value)
                if Value.DATE.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.DATE.value)
                
            elif columnType == stringType:
                symbolType = 4
                qvdFieldHeader.NumberFormat.Type = FieldType.ASCII

                if Value.ASCII.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.ASCII.value)
                if Value.TEXT.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.TEXT.value)

                
            elif columnType in floatingPointTypes:
                symbolType = 2
                qvdFieldHeader.NumberFormat.Type = FieldType.REAL
                
                if Value.NUMERIC.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.NUMERIC.value)                
                
            elif columnType in intTypes:
                symbolType = 1
                qvdFieldHeader.NumberFormat.Type = FieldType.INTEGER
                
                if Value.INTEGER.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.INTEGER.value)
                if Value.NUMERIC.value not in qvdFieldHeader.Tags.String:
                    qvdFieldHeader.Tags.String.append(Value.NUMERIC.value)

            B6 = struct.pack('B', 6)
            B5 = struct.pack('B', 5)
            B4 = struct.pack('B', 4)
            B2 = struct.pack('B', 2)
            B1 = struct.pack('B', 1)
            
            i = 0
            uniqueValues = column.unique()
            symbolBytes = bytearray()
            
            #construct symbol list
            for index, value in enumerate(uniqueValues):
                value = value.as_py()
                if value is not None:               
                    qvdFieldHeader._Symbol[value] = i
                    i +=1

                    if symbolType==6:                       
                        symbolBytes += B6
                        symbolBytes += struct.pack('<d', (value - value.date()).days)
                        symbolBytes += value.strftime("%H:%M:%S").encode('utf-8')
                        symbolBytes += b'\x00'

                    elif symbolType==66:        
                        symbolBytes += B6
                        symbolBytes += struct.pack('<d', (value - datetime(1900, 1, 1)).days)
                        symbolBytes += value.strftime("%Y-%m-%d %H:%M:%S").encode('utf-8')
                        symbolBytes += b'\x00'                        

                    elif symbolType==5:                       
                        symbolBytes += B5
                        symbolBytes += struct.pack('<i', (value - datetime(1900, 1, 1)).days)
                        symbolBytes += value.strftime("%Y-%m-%d").encode('utf-8')
                        symbolBytes += b'\x00' 

                            
                    elif symbolType==4:
                        symbolBytes += B4
                        symbolBytes += value.encode('utf-8')
                        symbolBytes += b'\x00'

                    elif symbolType==2:
                        symbolBytes += B2
                        symbolBytes += struct.pack('<d', value)
                            
                    elif symbolType==1:
                        symbolBytes += B1
                        symbolBytes += struct.pack('<i',value)

            qvdFieldHeader._SymbolBytes += symbolBytes

            #update XML metadata
            qvdFieldHeader.NoOfSymbols = len(qvdFieldHeader._Symbol)
            
            qvdFieldHeader.Offset = offset
            qvdFieldHeader.BitOffset = bitOffset
            qvdFieldHeader.BitWidth = (qvdFieldHeader.NoOfSymbols-1).bit_length()
            qvdFieldHeader.Length  = len(qvdFieldHeader._SymbolBytes) if qvdFieldHeader._SymbolBytes is not None else 0
            qvdFieldHeader.Bias = -2 if qvdFieldHeader.NoOfSymbols==0 else 0
            
            if qvdFieldHeader.NoOfSymbols == 1:
                qvdFieldHeader.BitOffset = 0
                qvdFieldHeader.BitWidth = 0

            bitOffset += qvdFieldHeader.BitWidth
            offset += qvdFieldHeader.Length 

            
        self.qvdTableHeader.Offset = offset
        self.qvdTableHeader.RecordByteSize = bitOffset // 8 + (1 if bitOffset %8>0 else 0)
        self.qvdTableHeader.Length = self.qvdTableHeader.RecordByteSize * self.qvdTableHeader.NoOfRecords
        
        
        padBitWidth = 8- (bitOffset % 8) if bitOffset % 8 >0 else 0
        
        resultColumn = None
        paddedFlag = False
        i = 0
        for qvdFieldHeader in self.qvdTableHeader.Fields.QvdFieldHeader: 
            #pad the bits to make byte 
            if not paddedFlag:
                if (qvdFieldHeader.BitOffset + qvdFieldHeader.BitWidth + padBitWidth) % 8 == 0 and  qvdFieldHeader.NoOfSymbols >1:
                    qvdFieldHeader.BitWidth += padBitWidth
                    paddedFlag = True
            else:
                if qvdFieldHeader.BitOffset > 0:
                    qvdFieldHeader.BitOffset += padBitWidth
        
            
            indexes = None
            
            #convert to value to index and shift it to the bitoffset
            if qvdFieldHeader.NoOfSymbols >1:

                if resultColumn is None:
                    resultColumn = [qvdFieldHeader._Symbol.get(x.as_py(), 0) << qvdFieldHeader.BitOffset for x in batch.column(i)]
                else:
                    resultColumn = [(qvdFieldHeader._Symbol.get(x.as_py(), 0) << qvdFieldHeader.BitOffset)| y for x, y in zip(batch.column(i), resultColumn)]
            i += 1   

            
        #prepare the record bytes
        self.recordBytes = bytearray()
        if resultColumn is None:
            self.recordBytes = b'\x00'
        else:
            for j in range(len(resultColumn)):                
                self.recordBytes += struct.pack(f'<{self.qvdTableHeader.RecordByteSize}B', *(resultColumn[j]>> (8 * i) & 0xFF for i in range(self.qvdTableHeader.RecordByteSize)))
        
     
    def WriteQVD(self):
        
        #write XML
        qvdXMLParser = QVDXMLParser()
        qvdXMLParser.WriteQVDXML(self.qvdTableHeader, self.qvdFile)
        
        #write the record bytes
        with open(self.qvdFile, 'ab') as fs:
            fs.write(b'\r\n\x00')
            for qvdFieldHeader in self.qvdTableHeader.Fields.QvdFieldHeader:
                fs.write(qvdFieldHeader._SymbolBytes)
            fs.write(self.recordBytes)
        
        self.recordBytes = None
        self.qvdTableHeader = None