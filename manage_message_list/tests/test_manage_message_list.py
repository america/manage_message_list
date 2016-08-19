#!/usr/bin/env python
# -*- coding: utf-8 ^*-

from nose.tools import eq_, raises

from dbutil.dbUtil import dbUtil

from manage_message_list.manage_message_list import manage_message_list
from collections import namedtuple
from unittest.mock import patch
from dbutil.constants import constants
import traceback


class test_manage_message_list():

    def setup(self):
        self.con = dbUtil.connect()
        self.con_for_create = dbUtil.connect()

        dbUtil.create_database(self.con_for_create,
                               'test_database')  # 暗黙的なコミット発生

        self.con.select_db('test_database')
        dbUtil.create_table(self.con_for_create,
                            'test_table_for_manage')  # 暗黙的なコミット発生

        # self.con.begin()

    def teardown(self):
        dbUtil.delete_table(self.con_for_create,
                            'test_table_for_manage')  # 暗黙的なコミット発生

        dbUtil.drop_database(self.con_for_create,
                             'test_database')  # 暗黙的なコミット発生
        # self.con.commit()

        dbUtil.disConnect(self.con)

    def test_01_insert(self):
        expected = True

        target = manage_message_list()

        Args = namedtuple('Args', 'table_name message')
        args = Args('test_table_for_manage', 'test_message')

        actual = target.insert(args)

        eq_(actual, expected)

    def test_02_insert_not_exist_table(self):
        expected = False

        target = manage_message_list()

        Args = namedtuple('Args', 'table_name message')
        args = Args('wrong_table_name', 'test_message')
        actual = target.insert(args)

        eq_(actual, expected)

    '''
    def test_03_insert_rollback(self):
        expected = ()

        target = manage_message_list()

        Args = namedtuple('Args', 'table_name message')
        args = Args('test_table_for_manage', 'test_message')
        target.insert(args)

        ShowAllArgs = namedtuple('ShowAllArgs', 'table_name')
        args = ShowAllArgs('test_table_for_manage')
        actual = target.show_all_msgs(args)
        eq_(actual, expected)
    '''

    def test_04_delete_answer_yes(self):
        expected = True

        target = manage_message_list()

        InsertArgs = namedtuple('InsertArgs', 'table_name message')
        args = InsertArgs('test_table_for_manage', 'test_message')
        target.insert(args)

        # self.con.commit()
        # target.con.commit()

        DeleteArgs = namedtuple('DeleteArgs', 'table_name no message')
        args = DeleteArgs('test_table_for_manage', [1], 'test_message')
        with patch('builtins.input', return_value='y'):
            actual = target.delete(args)

        eq_(actual, expected)

    def test_05_delete_answer_no(self):
        expected = False

        target = manage_message_list()

        InsertArgs = namedtuple('InsertArgs', 'table_name message')
        args = InsertArgs('test_table_for_manage', 'test_message')
        target.insert(args)

        DeleteArgs = namedtuple('DeleteArgs', 'table_name no message')
        args = DeleteArgs('test_table_for_manage', [1], 'test_message')
        with patch('builtins.input', return_value='n'):
            actual = target.delete(args)

        eq_(actual, expected)

        args = DeleteArgs('test_table_for_manage', [1], 'test_message')
        with patch('builtins.input', return_value='y'):
            actual = target.delete(args)

    def test_06_delete_answer_no_not_exist_msg(self):
        expected = False

        target = manage_message_list()

        DeleteArgs = namedtuple('DeleteArgs', 'table_name no message')
        args = DeleteArgs('test_table_for_manage', [1], 'test_message')
        with patch('builtins.input', return_value='n'):
            actual = target.delete(args)

        eq_(actual, expected)

    def test_07_delete_answer_yes_not_exist_table(self):
        expected = False

        target = manage_message_list()

        DeleteArgs = namedtuple('DeleteArgs', 'table_name no message')
        args = DeleteArgs('wrong_table_name', [1], 'test_message')
        with patch('builtins.input', return_value='n'):
            actual = target.delete(args)

        eq_(actual, expected)

    def test_08_show_all_msgs(self):
        self.con.begin()
        expected = ([1, 2], ['test_message01', 'test_message02'])

        target = manage_message_list()

        InsertArgs = namedtuple('InsertArgs', 'table_name message')
        args = InsertArgs('test_table_for_manage', 'test_message01')
        target.insert(args)
        args = InsertArgs('test_table_for_manage', 'test_message02')
        target.insert(args)

        ShowAllArgs = namedtuple('ShowAllArgs', 'table_name')
        args = ShowAllArgs('test_table_for_manage')

        actual = target.show_all_msgs(args)

        eq_(actual, expected)

        DeleteArgs = namedtuple('DeleteArgs', 'table_name no message')
        args = DeleteArgs('test_table_for_manage', [1], 'test_message01')
        with patch('builtins.input', return_value='y'):
            actual = target.delete(args)

        args = DeleteArgs('test_table_for_manage', [1], 'test_message01')
        with patch('builtins.input', return_value='y'):
            actual = target.delete(args)
        self.con.commit()
    '''
    def test_09_show_all_msgs_not_exist_msg(self):
        self.con.begin()
        # expected = (None, None)

        target = manage_message_list()

        ShowAllArgs = namedtuple('ShowAllArgs', 'table_name')
        args = ShowAllArgs('test_table_for_manage')

        # actual = target.show_all_msgs(args)

        # eq_(actual, expected)

        self.con.commit()

    '''

    def test_10_show_all_msgs_not_exist_table(self):
        self.con.begin()

        expected = ()

        target = manage_message_list()

        ShowAllArgs = namedtuple('ShowAllArgs', 'table_name')
        args = ShowAllArgs('wrong_table_name')

        constants.SELECT_ALL_MSG_SQL = 'sql/select_all_tables_manage.sql'
        actual = target.show_all_msgs(args)

        eq_(actual, expected)

        self.con.commit()

    '''
    def test_11_show_all_tables(self):
        self.con.begin()
        expected = 1

        target = manage_message_list()

        args = None
        all_tables = target.show_all_tables(args)

        actual = len(all_tables)

        eq_(actual, expected)

        self.con.commit()
    '''

    @raises(OSError)
    def test_12_show_all_tables_err(self):

        try:
            target = manage_message_list()

            constants.SELECT_ALL_TABLES_SQL = 'not_exist_file'
            args = None
            target.show_all_tables(args)
        finally:
            constants.SELECT_ALL_TABLES_SQL = "sql/select_all_tables.sql"

    def test_13_earch(self):
        expected = ([1], ['test_message01'], 'test_table_for_manage')
        target = manage_message_list()

        InsertArgs = namedtuple('InsertArgs', 'table_name message')
        args = InsertArgs('test_table_for_manage', 'test_message01')
        target.insert(args)

        SearchArgs = namedtuple('SearchArgs', 'keyword')
        args = SearchArgs('test_message01')
        actual = target.search(args)

        print(actual)
        eq_(actual, expected)

        DeleteArgs = namedtuple('DeleteArgs', 'table_name no message')
        args = DeleteArgs('test_table_for_manage', [1], 'test_message')
        with patch('builtins.input', return_value='y'):
            actual = target.delete(args)

    '''
    def test_14_search_not_hit(self):
        expected = (None, None, None)

        target = manage_message_list()

        SearchArgs = namedtuple('SearchArgs', 'keyword')
        args = SearchArgs('test_message01')
        actual = target.search(args)

        eq_(actual, expected)

    '''

    '''
    def test_15_create_table(self):
        self.con.begin()
        expected = True

        target = manage_message_list()

        CreateTableArgs = namedtuple('CreateTableArgs', 'tablename')
        args = CreateTableArgs('test_table_for_create')
        actual = target.create_table(args)

        # self.con.begin()
        eq_(actual, expected)
    '''

    def test_16_create_table_already_exists(self):
        self.con_for_create.begin()
        expected = False

        target = manage_message_list()

        CreateTableArgs = namedtuple('CreateTableArgs', 'tablename')
        args = CreateTableArgs('test_table_for_create')
        target.create_table(args)
        actual = target.create_table(args)

        eq_(actual, expected)

        dbUtil.delete_table(self.con, 'test_table_for_create')

    '''
    @raises(FileNotFoundError)
    def test_17_create_table_err(self):

        target = manage_message_list()

        CreateTableArgs = namedtuple('CreateTableArgs', 'tablename')
        args = CreateTableArgs('test_table_for_create')

        try:
            constants.CREATE_TABLE_DDL = 'not_exist_file'
            target.create_table(args)
        except Exception as e:
            print(e)
        finally:
            constants.CREATE_TABLE_DDL = 'sql/create_table.ddl'
    '''

    '''
    def test_18_delete_table(self):
        # self.con.begin()
        expected = True

        target = manage_message_list()

        dbUtil.create_table(self.con_for_create, 'test_table_for_delete')

        DeleteTableArgs = namedtuple('DeleteTableArgs', 'tablename')
        args = DeleteTableArgs('test_table_for_delete')
        with patch('builtins.input', return_value='y'):
            actual = target.delete_table(args)

        eq_(actual, expected)
    '''

    '''
    def test_19_delete_table_not_exist(self):
        self.con.begin()

        expected = False

        target = manage_message_list()

        DeleteTableArgs = namedtuple('DeleteTableArgs', 'tablename')
        args = DeleteTableArgs('test_table_for_delete')
        with patch('builtins.input', return_value='y'):
            actual = target.delete_table(args)

        eq_(actual, expected)
    '''

    '''
    @raises(FileNotFoundError)
    def test_20_delete_table_err(self):

        target = manage_message_list()

        dbUtil.create_table(self.con_for_create, 'test_table_for_delete')

        DeleteTableArgs = namedtuple('DeleteTableArgs', 'tablename')
        args = DeleteTableArgs('test_table_for_delete')

        try:
            with patch('builtins.input', return_value='y'):
                constants.DROP_TABLE_DDL = 'not_exist_file'
                target.delete_table(args)

        finally:
            constants.DROP_TABLE_DDL = 'sql/drop_table.ddl'
            # dbUtil.delete_table(self.con, 'test_table_for_delete')
    '''
