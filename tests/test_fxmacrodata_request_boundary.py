"""Offline tests of the actual vendor request path with unrelated framework stubs."""
import importlib.util
import logging
from pathlib import Path
import sys
import traceback
import types
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        base=types.ModuleType('findatapy.market.datavendor')
        base.DataVendor=type('DataVendor',(),{})
        util=types.ModuleType('findatapy.util')
        util.DataConstants=lambda:types.SimpleNamespace(fxmacrodata_base_url='https://example.invalid/v1')
        util.LoggerManager=lambda:types.SimpleNamespace(getLogger=logging.getLogger)
        packages={}
        for name in ['findatapy','findatapy.market']:
            module=types.ModuleType(name);module.__path__=[];packages[name]=module
        packages.update({'findatapy.market.datavendor':base,'findatapy.util':util})
        path=Path(__file__).resolve().parents[1]/'findatapy/market/datavendorfxmacrodata.py'
        spec=importlib.util.spec_from_file_location('boundary_vendor',path)
        self.module=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,packages):spec.loader.exec_module(self.module)
        self.vendor=self.module.DataVendorFXMacroData()
        self.request=types.SimpleNamespace(start_date='2026-01-01',finish_date='2026-01-03',fxmacrodata_api_key='fixture-key')

    def download(self):
        return self.vendor._download_fx_pair(self.request,'EURUSD',['close'])

    def response(self,payload):
        return types.SimpleNamespace(ok=True,status_code=200,json=Mock(return_value=payload))

    def test_invalid_payloads_do_not_become_empty_success(self):
        for payload in ({'error':'fixture-private-value'},{'detail':'fixture-private-value'},{},None):
            with self.subTest(kind=type(payload).__name__),patch.object(requests,'get',return_value=self.response(payload)):
                with self.assertRaises(ValueError) as raised:self.download()
                self.assertNotIn('fixture-private-value',str(raised.exception))

    def test_private_transport_reason_is_not_chained(self):
        with patch.object(requests,'get',side_effect=requests.ConnectionError('fixture-private-value')):
            try:self.download()
            except RuntimeError as exc:self.assertNotIn('fixture-private-value',''.join(traceback.format_exception(exc)))
            else:self.fail('failure suppressed')

    def test_private_json_error_is_not_chained(self):
        response=self.response(None);response.json.side_effect=ValueError('fixture-private-value')
        with patch.object(requests,'get',return_value=response):
            try:self.download()
            except ValueError as exc:self.assertNotIn('fixture-private-value',''.join(traceback.format_exception(exc)))
            else:self.fail('failure suppressed')

    def test_short_page_with_more_data_is_not_truncated(self):
        first={'data':[{'date':'2026-01-01','val':1.0}], 'pagination':{'has_more':True,'next_offset':1}}
        second={'data':[{'date':'2026-01-02','val':1.1}], 'pagination':{'has_more':False}}
        with patch.object(requests,'get',side_effect=[self.response(first),self.response(second)]) as get:
            frame=self.download()
        self.assertIsInstance(frame,pd.DataFrame)
        self.assertEqual([1.0,1.1],frame['close'].tolist())
        self.assertEqual(2,get.call_count)

    def test_bad_next_offset_is_rejected_before_repetition(self):
        for offset in (0,-1,'bad',True,1.5):
            payload={'data':[{'date':'2026-01-01','val':1.0}], 'pagination':{'has_more':True,'next_offset':offset}}
            with self.subTest(offset=offset),patch.object(requests,'get',return_value=self.response(payload)):
                with self.assertRaises(ValueError):self.download()

    def test_existing_redirect_and_header_guards_are_preserved(self):
        response=types.SimpleNamespace(ok=True,status_code=302,json=Mock())
        with patch.object(requests,'get',return_value=response) as get:
            with self.assertRaises(RuntimeError):self.download()
        self.assertFalse(get.call_args.kwargs['allow_redirects'])
        self.assertEqual('fixture-key',get.call_args.kwargs['headers']['X-API-Key'])
        response.json.assert_not_called()

    def test_legacy_empty_response_stays_unavailable(self):
        with patch.object(requests,'get',return_value=self.response({'data':[]})):
            self.assertIsNone(self.download())


if __name__=='__main__':unittest.main()
