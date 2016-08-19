#!/usr/bin/env/python
# -*- coding: utf-8 -*-

import sys

from dbutil.dbUtil import dbUtil
import traceback
from logging import getLogger, StreamHandler, Formatter, FileHandler, INFO, \
     DEBUG
import argparse
from dbutil.constants import constants
from dbutil.util.deco import logging
import pymysql


class manage_message_list():

    con = None

    @logging
    def __init__(self, logger=None):
        # logger for stdlog
        self.logger = logger if logger else getLogger("log")
        self.logger.setLevel(INFO)
        self.handler = StreamHandler()
        self.handler.setFormatter(Formatter(fmt='%(levelname)s %(message)s'))
        self.logger.addHandler(self.handler)

        self.con = dbUtil.connect()

    @logging
    def insert(self, args):

        self.con.begin()
        table_name = args.table_name
        message = args.message

        if self.exist_table(table_name):
            try:
                no = dbUtil.insert_message(self.con, table_name, message)
                if no:
                    self.logger.info(constants.SEPARATE_LINE)
                    self.logger.info("'" + message + "'")
                    self.logger.info(constants.INSERT_MSG + table_name +
                                     " at No: " + str(no))
                    self.logger.info(constants.SEPARATE_LINE)

            except Exception:
                self.con.commit()
                raise
            else:
                self.con.commit()
                return True
        else:
            self.con.commit()
            return False

    @logging
    def delete(self, args):

        self.con.begin()

        table_name = args.table_name
        no_list = args.no

        if self.exist_table(table_name):

            for no in no_list:
                try:
                    msg = dbUtil.get_single_msg(self.con, table_name, no)

                    if msg:

                        if self.yes_no_input(table_name, no, msg):
                            dbUtil.delete_message(self.con, table_name, no)

                            self.logger.info(constants.SEPARATE_LINE)
                            self.logger.info("table_name: " + table_name)
                            self.logger.info("no: " + str(no))
                            self.logger.info("msg: " + msg)
                            self.logger.info(constants.DELETE_MSG + table_name)
                            self.logger.info(constants.SEPARATE_LINE)

                            self.con.commit()

                            return True
                        else:
                            return False

                    else:
                        self.logger.info(constants.SEPARATE_LINE)
                        self.logger.info(constants.NOT_EXIST_MSG)
                        self.logger.info(constants.SEPARATE_LINE)

                        self.con.commit()

                        return False
                except Exception:
                    raise

        else:
            return False

    @logging
    def show_all_msgs(self, args):
        self.con.begin()
        table_name = args.table_name

        no_list = None
        msg_list = None

        if self.exist_table(table_name):

            try:
                results = dbUtil.getAllMsgs(self.con, table_name)
                if results:
                    (no_list, msg_list) = dbUtil.getAllMsgs(self.con,
                                                            table_name)

                    # logger for message list
                    self.list_logger = \
                        self.make_filehandler_logger(table_name,
                                                     'message_list')

                    index = 0
                    while index < len(no_list):
                        self.logger.info(constants.SEPARATE_LINE)
                        self.logger.info("no: " + str(no_list[index]))
                        self.logger.info("msg: " + str(msg_list[index]))
                        self.logger.info(constants.SEPARATE_LINE)

                        self.list_logger.info(constants.SEPARATE_LINE)
                        self.list_logger.info("no: " + str(no_list[index]))
                        self.list_logger.info("msg: " + str(msg_list[index]))
                        self.list_logger.info(constants.SEPARATE_LINE)

                        index += 1

            except Exception:
                self.con.rollback()
                raise
            else:
                self.con.commit()
                return (no_list, msg_list)
        else:
            self.con.commit()
            return ()

    @logging
    def show_all_tables(self, args):

        try:
            all_tables = dbUtil.get_all_tables(self.con)

            self.logger.info(constants.SEPARATE_LINE)
            cnt = 1
            for table_name in all_tables:
                self.logger.info(str(cnt) + ": " + table_name)
                cnt += 1

            self.logger.info(constants.SEPARATE_LINE)

        except Exception:
            raise
        else:
            return all_tables

    @logging
    def search(self, args):

        keyword = args.keyword

        no_list = None
        msg_list = None
        table_name = None

        try:
            result_lists = dbUtil.search_msg_by_kword(self.con, keyword)
            if result_lists:

                # logger for keyword list file
                self.list_logger = self.make_filehandler_logger(keyword,
                                                                'keyword_list')

                for nt in result_lists:
                    no_list = nt.nos
                    msg_list = nt.msgs
                    table_name = nt.table_name

                    index = 0
                    while index < len(no_list):
                        self.logger.info(constants.SEPARATE_LINE)
                        self.logger.info("table_name: " + table_name)
                        self.logger.info("no: " + str(no_list[index]))
                        self.logger.info("msg: " + msg_list[index])
                        self.logger.info(constants.SEPARATE_LINE)

                        self.list_logger.info(constants.SEPARATE_LINE)
                        self.list_logger.info("table_name: " + table_name)
                        self.list_logger.info("no: " + str(no_list[index]))
                        self.list_logger.info("msg: " + msg_list[index])
                        self.list_logger.info(constants.SEPARATE_LINE)

                        index += 1
            # else:
            #    return ()
        except Exception:
            raise
        else:
            return (no_list, msg_list, table_name)

    def create_table(self, args):

        self.con.begin()
        table_name = args.tablename

        if not self.exist_table(table_name):
            try:
                dbUtil.create_table(self.con, table_name)
                self.logger.info(constants.SEPARATE_LINE)
                self.logger.info(constants.TABLE_CREATED_MSG.replace('table_name', table_name))
                self.logger.info(constants.SEPARATE_LINE)

            except Exception:
                raise
            else:
                return True
        else:
            self.logger.error(constants.SEPARATE_LINE)
            self.logger.error(constants.TABLE_ALREADY_EXIST_MSG.replace('table_name', table_name))
            self.logger.error(constants.SEPARATE_LINE)

            self.con.commit()
            return False

    @logging
    def delete_table(self, args):

        self.con.begin()
        table_name = args.tablename

        if self.exist_table(table_name):
            try:
                if self.yes_no_input(table_name):
                    dbUtil.delete_table(self.con, table_name)
                    self.logger.info(constants.SEPARATE_LINE)
                    self.logger.info(constants.TABLE_DELETED_MSG.replace('table_name', table_name))
                    self.logger.info(constants.SEPARATE_LINE)

            except Exception:
                raise
            else:
                return True
        else:
            self.logger.error(constants.SEPARATE_LINE)
            self.logger.error(constants.TABLE_NOT_EXIST_MSG.replace('table_name', table_name))
            self.logger.error(constants.SEPARATE_LINE)

            return False

    @logging
    def yes_no_input(self, table_name, no=None, msg=None):

        self.logger.info(constants.SEPARATE_LINE)
        self.logger.info("table_name: " + table_name)
        if no and msg:
            self.logger.info("no: " + str(no))
            self.logger.info("msg: " + msg)

        while True:
            if no and msg:
                choice = input(constants.CONFIRM_DELETE_MSG_MSG).lower()
            else:
                choice = input(constants.CONFIRM_DELETE_TABLE_MSG).lower()

            if choice in ['y', 'ye', 'yes']:
                return True
            elif choice in ['n', 'no']:
                return False

    @logging
    def exist_table(self, table_name):

        all_tables = dbUtil.get_all_tables(self.con)

        if table_name not in all_tables:
            self.logger.error(constants.SEPARATE_LINE)
            self.logger.error(constants.TABLE_NOT_EXIST_MSG.replace('table_name', table_name))
            self.logger.error(constants.SEPARATE_LINE)
            return False
        else:
            return True

    @logging
    def make_filehandler_logger(self, handler_prefix, logger_name):

        list_logger = getLogger('keyword_list')
        list_logger.setLevel(DEBUG)
        list_handler = FileHandler(handler_prefix + '_list.log', 'w',
                                   encoding='utf-8')
        list_logger.addHandler(list_handler)

        return list_logger

if __name__ == '__main__':

    def _parse():
        parser = argparse.ArgumentParser()
        subparser = parser.add_subparsers(help='sub-command help')

        # create the parser for the insert command
        parser_insert = subparser.add_parser('insert', help='insert table_name massage')
        parser_insert.set_defaults(func=manager.insert)
        parser_insert.add_argument('table_name')
        parser_insert.add_argument('message')

        # create the parser for the show command
        parser_show = subparser.add_parser('show', help='show table_name ')
        parser_show.set_defaults(func=manager.show_all_msgs)
        parser_show.add_argument('table_name')

        # create the parser for the delete command
        parser_delete = subparser.add_parser('delete', help='delete table_name no')
        parser_delete.set_defaults(func=manager.delete)
        parser_delete.add_argument('table_name')
        parser_delete.add_argument('no', type=int, nargs='*')

        # create the parser for the show_tables command
        parser_show_tables = subparser.add_parser('show_tables', help='show_tables')
        parser_show_tables.set_defaults(func=manager.show_all_tables)

        # create the parser for the search command
        parser_search = subparser.add_parser('search', help='search keyword')
        parser_search.set_defaults(func=manager.search)
        parser_search.add_argument('keyword')

        # create the parser for the create_table command
        parser_create_table = subparser.add_parser('create_table', help='create_table tablename')
        parser_create_table.set_defaults(func=manager.create_table)
        parser_create_table.add_argument('tablename')

        # create the parser for the delete_table command
        parser_create_table = subparser.add_parser('delete_table', help='delete_table tablename')
        parser_create_table.set_defaults(func=manager.delete_table)
        parser_create_table.add_argument('tablename')
        args = parser.parse_args()

        has_func = hasattr(args, 'func')

        if not has_func:
            parser.parse_args(['-h'])
            return
        else:
            return args

    try:
        manager = manage_message_list()
        args = _parse()
        if args:
            result = args.func(args)

            sys.exit()
    except pymysql.InternalError as error:

        (code, message) = error.args

        # table already exists
        if code == 1050:
            manager.logger.error(constants.SEPARATE_LINE)
            manager.logger.error(message)
            manager.logger.error(constants.SEPARATE_LINE)

    except Exception:
        traceback.print_exc()
    finally:
        dbUtil.disConnect(manager.con)
