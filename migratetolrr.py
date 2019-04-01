import pandas as pd
import json
import sqlite3
import os
from random import randint
import copy
import re

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def db_connect(db_path=DEFAULT_PATH):  
    try:
        conn = sqlite3.connect(db_path)
    except Error as e:
        print(e)
    return conn

def get_filename_from_windows_path(path) -> str:
    # attempt to decode to unicode    
    try:
        path=path.decode('utf-8')
    except:
        None
        
    # get filename from path string    
    try:
        filename=path.split('\\')[-1].rsplit('.',1)[0]
    except: 
        filname=None
    return filename

def get_lrr_gallery(lrr,filename) -> dict:
    for g in lrr:
        if g['filename']==filename:
            return g
        else:
            return None

def get_data_from_pandaviewer(filepath) -> list:
    conn=db_connect(filepath)
    conn.row_factory=dict_factory
    
    query='select * from metadata left join gallery on metadata.gallery_id=gallery.id'
    cur=conn.cursor()
    cur.execute(query)
    return cur.fetchall()

def get_data_from_happypanda(filepath) -> list:
    conn=db_connect(filepath)
    conn.row_factory=dict_factory
    
    query=('select series.title, series.series_path, series.link, group_concat(namespaces.namespace || \':\' || tags.tag) as tags'
           ' from series'
           ' inner join series_tags_map on series.series_id=series_tags_map.series_id'
           ' inner join tags_mappings on tags_mappings.tags_mappings_id=series_tags_map.tags_mappings_id'
           ' inner join namespaces on namespaces.namespace_id=tags_mappings.namespace_id'
           ' inner join tags on tags.tag_id=tags_mappings.tag_id'
           ' group by series.title, series.series_path'
          )
    
    cur=conn.cursor()
    cur.execute(query)
    return cur.fetchall()

def get_data_from_lrr(lrrfilename) -> list:
    with open(lrrfilename) as infile:
        lrr=json.load(infile)
    return lrr

# might want to change all these pv functions into class methods for pandaviewer object

def get_pv_tags(pvjson) -> str:
    if pvjson.get('tags'):
        tags=', '.join(pvjson.get('tags'))
    else:
        tags=''
    return tags
        
def get_pv_panda_url(pvjson) -> str:
    gid=pvjson.get('gid')
    token=pvjson.get('token')
    if gid and token:
        pandaurl='http://exhentai.org/g/'+str(gid)+'/'+str(token)
    else:
        pandaurl=''
    return pandaurl

def fix_magazine_names(lrrdata) -> list:
    comicstofix={':girls form',':angel club', ':x-eros'} # need to change to regex matching for better robustness
    newlrrdata=copy.deepcopy(lrrdata)
    for g in newlrrdata:
        # fix magazine tags to include the word 'comic'
        tagstr=g.get('tags')
        for ss in comicstofix:
            pos=tagstr.lower().find(ss)
            if pos>=0:
                tagstr=tagstr[:pos+1]+'COMIC '+tagstr[pos+1:]
                
        g['tags']=tagstr # maybe use enumerate and index to explicity modify list entry
        
        # change magazine namespace to 'series:'
        taglist=g.get('tags').split(', ')
        for idx,tag in enumerate(taglist):
            if 'comic ' in tag.lower():
                taglist[idx]=re.sub('.*:','series:',tag)
        g['tags']=', '.join(taglist)
    return newlrrdata

def replace_with_pandaviewer(pvdata,lrrdata,usetitle=False,usetags=False) -> list:
    newlrrdata=copy.deepcopy(lrrdata)
    # main logic for replacing lrr metadata with pandaviewer data
    for pvg in pvdata:
        if pvg['name']=='gmetadata': # only use e-h metadata
            pvfilename=get_filename_from_windows_path(pvg['path'])
            # get lrr gallery entry matching pandaviewer
            lrrgallery=next((g for g in newlrrdata if g['filename']==pvfilename),None) 
            if lrrgallery:
                pvjson=json.loads(pvg['json'])
                pvtagstr=get_pv_tags(pvjson)

                # get info from pandaviewer
                pvtitle=pvjson.get('title') 
                if not pvtitle:
                    pvtitle=pvfilename # need to use filename as title if field blank
                pvtitle_jpn=pvjson.get('title_jpn') # not used by lrr currently
                pandaurl=get_pv_panda_url(pvjson)

                # replace title and certain namespace tags from pandaviewer
                if usetitle:
                    lrrgallery['title']=pvtitle
                if pvtagstr and usetags:
                    if pvfilename!=lrrgallery['filename']:
                        print(pvfilename,'doesn\'t match', lrrgallery['filename'])
                        raise Exception('file mismatch')
                    lrrtaglist=lrrgallery.get('tags').split(', ')
                    lrrtaglist=[tag for tag in lrrtaglist if 'event:' in tag or 'series:' in tag]
                    pvtaglist=pvtagstr.split(', ')
                    newtaglist=set(lrrtaglist+pvtaglist)
                    newtagstr=', '.join(newtaglist)
                    lrrgallery['tags']=newtagstr

    return newlrrdata
                
def replace_with_happypanda(hpdata,lrrdata,replacetitle=False,replacetags=False) -> list:
    newlrrdata=copy.deepcopy(lrrdata)
    # main logic for replacing lrr metadata with happypanda data
    for hpg in hpdata:
        hpfilename=get_filename_from_windows_path(hpg['series_path'])
        # get lrr gallery entry matching pandaviewer
        for g in newlrrdata:
            if hpfilename==g['filename']:
                lrrgallery=g
                break

        hptagstr=hpg.get('tags').lower().replace(',',', ')

        # get info from happypanda
        hptitle=hpg.get('title')
        if not hptitle:
            hptitle=hpfilename
        pandaurl=hpg.get('link')

        # replace title and tags from pandaviewer
        if replacetitle:
            lrrgallery['title']=hptitle
        if pvtagstr and replacetags:
            lrrgallery['tags']=hptagstr # check if tags are problematic too (only one non-existent file)
    
    return newlrrdata