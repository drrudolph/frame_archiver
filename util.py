#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:41:34 2018

@author: daniel
"""
import os
import email
from smtplib import SMTP

class Error(Exception):
    """Base class for exceptions in this module."""
    pass


def yes_or_no(question):
    reply = str(input(question+' (y/n): ')).lower().strip()
    if reply[0] == 'y':
        return True
    if reply[0] == 'n':
        return False
#    else:
#        return yes_or_no("please enter y or n")


def mail_message(message, address):
    """mail error messages to admin"""
    msg = email.message_from_string(message)
    msg['Subject'] = 'frame archiver: '
    msg['From'] = 'python@ema3013'
    msg['To'] = address
    server = SMTP('localhost')
    server.sendmail("From: ", "To: ", message)
    server.quit()


def script_fail(errormsg="unknown error"):
    """"""
    print("script failed with error:", errormsg)
    
    
def get_dir_size(path):
    """https://gist.github.com/SteveClement/3755572"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size