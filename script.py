import xlwings  as xw
import os
import win32com.client as client


from PIL import ImageGrab
import excel2img
import time





def recap_1_1():
   # user = xw.Book.Application.user 
    wb = xw.Book.caller()
    ws = wb.sheets['recap_1_1']
    win32c = client.constants
    crange =ws.range('A1:p75')
    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png','PNG')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """   
    <div> 
    
    </div> 
     <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>    
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To= a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()


def recap_1_2():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_1_2']
    win32c = client.constants
    crange = ws.range('A1:p75')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png','PNG')
    crange2 = ws.range('A76:p150')
   
    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png','PNG')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
  
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()



def recap_2_1():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_2_1']
    win32c = client.constants
    crange =ws.range('A1:p75')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """   
    <div> 
   
    </div> 
     <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>    
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To= a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()


def recap_2_2():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_2_2']
    win32c = client.constants
    crange = ws.range('A1:p75')
    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    crange2 = ws.range('A76:p150')

    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
  
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()


def recap_2_2():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_2_2']
    win32c = client.constants
    crange = ws.range('A1:p75')
    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    crange2 = ws.range('A76:p150')

    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 

    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()


def recap_3_1():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_3_1']
    win32c = client.constants
    crange =ws.range('A1:p75')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """   
    <div> 
    <h1>bonjour</h1>    
    </div> 
     <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>    
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To= a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()


def recap_3_2():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_3_2']
    win32c = client.constants
    crange = ws.range('A1:p75')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    crange2 = ws.range('A76:p150')

    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
    <h1>bonjour</h1>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()

def recap_4_1():
        wb = xw.Book.caller()
        ws = wb.sheets['recap_4_1']
        win32c = client.constants
        crange = ws.range('A1:p75')
        time.sleep(1)
        crange.api.Copy()
        ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
        suject = ws.range('R1').value
        a = ws.range('R2').value
        cc = ws.range('R3').value
        html_body = """   
        <div> 
        <h1>bonjour</h1>    
        </div> 
         <div> 
        <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>    
        </div> 
        """
        outlook = client.Dispatch('Outlook.Application')
        message = outlook.createitem(0)
        message.To = a
        message.Cc = cc
        message.Subject = suject
        message.HTMLBody = html_body
        message.Display()

def recap_4_2():
        wb = xw.Book.caller()
        ws = wb.sheets['recap_4_2']
        win32c = client.constants
        crange = ws.range('A1:p75')
        crange.api.Copy()
        time.sleep(1)
        ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
        crange2 = ws.range('A76:p150')
        crange2.api.Copy()
        time.sleep(1)
        ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png')
        suject = ws.range('R1').value
        a = ws.range('R2').value
        cc = ws.range('R3').value
        html_body = """
        <div> 
        <h1>bonjour</h1>
        </div> 
        <div> 
        <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
        </div> 
        <div> 
        <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
        </div> 
        """
        outlook = client.Dispatch('Outlook.Application')
        message = outlook.createitem(0)
        message.To = a
        message.Cc = cc
        message.Subject = suject
        message.HTMLBody = html_body
        message.Display()

def recap_5_1():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_5_1']
    win32c = client.constants
    crange = ws.range('A1:p75')
    time.sleep(1)
    crange.api.Copy()
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """   
    <div> 
    <h1>bonjour</h1>    
    </div> 
     <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>    
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()

def recap_5_2():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_5_2']
    win32c = client.constants
    crange = ws.range('A1:p75')
    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    crange2 = ws.range('A76:p150')
    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
    <h1>bonjour</h1>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()

def recap_6_1():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_6_1']
    win32c = client.constants
    crange = ws.range('A1:p75')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """   
    <div> 
    <h1>bonjour</h1>    
    </div> 
     <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>    
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()

def recap_6_2():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_6_2']
    win32c = client.constants
    crange = ws.range('A1:p75')

    crange.api.CopyPicture(Format=win32c.xlBitmap)
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    crange2 = ws.range('A76:p150')

    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
   
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()

def recap_7_1():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_7_1']
    win32c = client.constants
    crange = ws.range('A1:p75')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """   
    <div> 
     
    </div> 
     <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>    
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()

def recap_7_2():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_7_2']
    win32c = client.constants
    crange = ws.range('A1:p75')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    crange2 = ws.range('A76:p150')

    crange2.api.CopyPicture(Format=win32c.xlBitmap)
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
  
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()

def recap_8_1():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_8_1']
    win32c = client.constants
    crange = ws.range('A1:p75')
    time.sleep(1)
    crange.api.Copy()
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """   
    <div> 
     
    </div> 
     <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>    
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()

def recap_8_2():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_8_2']
    win32c = client.constants
    crange = ws.range('A1:p75')
    time.sleep(1)
    crange.api.Copy()
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png')
    crange2 = ws.range('A76:p150')

    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
   
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()
    
    
    
def recap_1_3():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_1_3']
    win32c = client.constants
    crange = ws.range('A1:p75')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png','PNG')
    
    
    
    crange2 = ws.range('A76:p150')
   
    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png','PNG')
    
    
    
    
    
    crange3 = ws.range('A151:p220')
   
    crange3.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap3.png','PNG')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap3.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()


def recap_2_3():
    wb = xw.Book.caller()
    ws = wb.sheets['recap_2_3']
    win32c = client.constants
    crange = ws.range('A1:p70')

    crange.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap.png', 'PNG')

    crange2 = ws.range('A71:p140')

    crange2.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap2.png', 'PNG')

    crange3 = ws.range('A141:p210')

    crange3.api.Copy()
    time.sleep(1)
    ImageGrab.grabclipboard().save(r'Y:\DCG_data warehouse\mail\images\recap3.png', 'PNG')
    suject = ws.range('R1').value
    a = ws.range('R2').value
    cc = ws.range('R3').value
    html_body = """
    <div> 
    </div> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap.png" ></img>
    </div> 
    
    <br> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap2.png" ></img>
    </div> 
    <br> 
    <div> 
    <img src="Y:\\DCG_data warehouse\\mail\\images\\recap3.png" ></img>
    </div> 
    """
    outlook = client.Dispatch('Outlook.Application')
    message = outlook.createitem(0)
    message.To = a
    message.Cc = cc
    message.Subject = suject
    message.HTMLBody = html_body
    message.Display()
