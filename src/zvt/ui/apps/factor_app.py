# -*- coding: utf-8 -*-
from typing import List

import pandas as pd
import dash_daq as daq
import dash_bootstrap_components as dbc
from dash import dash, dcc, html
from dash.dependencies import Input, Output, State

from zvt.contract import Mixin
from zvt.contract import zvt_context, IntervalLevel, AdjustType
from zvt.contract.api import get_entities, get_schema_by_name, get_schema_columns
from zvt.contract.drawer import StackedDrawer
from zvt.domain.meta.stock_meta import Stock, StockDetail
from zvt.domain.fundamental.valuation import StockValuation
from zvt.trader.trader_info_api import AccountStatsReader, OrderReader, get_order_securities
from zvt.trader.trader_info_api import get_trader_info
from zvt.trader.trader_schemas import TraderInfo
from zvt.ui import zvt_app
from zvt.ui.components.dcc_components import get_account_stats_figure, order_type_flag, order_type_color
from zvt.utils.pd_utils import pd_is_not_null
import logging

logger = logging.getLogger(__name__)

# Global cache for readers to avoid re-initializing on every callback
# In a real multi-user app, this should be handled differently (e.g. server-side cache),
# but this is sufficient for a local research tool.
account_readers = []
order_readers = []
traders: List[TraderInfo] = []
trader_names: List[str] = []
traders_loaded = False




def ensure_traders_loaded():
    global traders, trader_names, account_readers, order_readers, traders_loaded
    if not traders_loaded:
        logger.info("Lazy loading traders from database...")
        traders = get_trader_info(return_type="domain")
        account_readers.clear()
        order_readers.clear()
        for trader in traders:
            account_readers.append(AccountStatsReader(level=trader.level, trader_names=[trader.trader_name]))
            order_readers.append(
                OrderReader(start_timestamp=trader.start_timestamp, level=trader.level, trader_names=[trader.trader_name])
            )
        trader_names = [item.trader_name for item in traders]
        traders_loaded = True
        logger.info(f"Loaded {len(traders)} traders.")


def factor_layout():
    # Use dbc components for a cleaner layout
    layout = html.Div(
        [
            dcc.Store(id="init-store", data={}),
            dbc.Row(
                [
                    # Control Panel (Left column conceptually, but handled by main.py sidebar? No, factor_app controls go here)
                    # We will place controls in a wide top bar or a side column inside the main content area.
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader((html.H5("全局控制", className="m-0")), className="bg-dark text-light"),
                                    dbc.CardBody(
                                        [
                                            # select trader
                                            html.Div(
                                                [
                                                    dbc.Label("选择交易员 (可选):", className="text-light"),
                                                    dcc.Loading(
                                                        dcc.Dropdown(id="trader-selector", placeholder="选择一个交易策略以查看信号 / 统计")
                                                    )
                                                ],
                                                className="mb-3",
                                            ),
                                            # select entity type
                                            html.Div(
                                                [
                                                    dbc.Label("实体类型:", className="text-light"),
                                                    dcc.Dropdown(
                                                        id="entity-type-selector",
                                                        placeholder="选择实体类型",
                                                        options=[{"label": name, "value": name} for name in zvt_context.tradable_schema_map.keys()],
                                                        value="stock",
                                                        clearable=False,
                                                    ),
                                                ],
                                                className="mb-3",
                                            ),
                                            # select entity provider
                                            html.Div(
                                                [
                                                    dbc.Label("实体数据源:", className="text-light"),
                                                    dcc.Dropdown(id="entity-provider-selector", placeholder="选择数据源"),
                                                ],
                                                className="mb-3",
                                            ),
                                            # select entity
                                            html.Div(
                                                [
                                                    dbc.Label("实体标的:", className="text-light"),
                                                    dcc.Loading(dcc.Dropdown(id="entity-selector", placeholder="选择要绘图的实体标的")),
                                                ],
                                                className="mb-3",
                                            ),
                                        ]
                                    )
                                ],
                                className="mb-4 bg-secondary shadow",
                            ),
                            dbc.Card(
                                [
                                    dbc.CardHeader((html.H5("交易信号", className="m-0")), className="bg-dark text-light"),
                                    dbc.CardBody(
                                        [
                                            dcc.Loading(
                                                html.Div(id="signals-chart-container", children=[html.P("未选择交易员或未找到订单数据。", className="text-muted text-center mt-3")]),
                                                type="circle"
                                            )
                                        ]
                                    )
                                ],
                                className="mb-4 shadow"
                            ),
                            dbc.Card(
                                [
                                    dbc.CardHeader((html.H5("账户统计", className="m-0")), className="bg-dark text-light"),
                                    dbc.CardBody(
                                        [
                                            dcc.Loading(
                                                html.Div(id="stats-chart-container", children=[html.P("未选择交易员或未找到统计数据。", className="text-muted text-center mt-3")]),
                                                type="circle"
                                            )
                                        ]
                                    )
                                ],
                                className="mb-4 shadow"
                            ),
                            dbc.Card(
                                [
                                    dbc.CardHeader((html.H5("图表参数", className="m-0")), className="bg-dark text-light"),
                                    dbc.CardBody(
                                        [
                                            # select levels
                                            html.Div(
                                                [
                                                    dbc.Label("K线级别:", className="text-light"),
                                                    dcc.Dropdown(
                                                        id="levels-selector",
                                                        options=[{"label": level.name, "value": level.value} for level in (IntervalLevel.LEVEL_1WEEK, IntervalLevel.LEVEL_1DAY)],
                                                        value="1d",
                                                        multi=True,
                                                    ),
                                                ],
                                                className="mb-3",
                                            ),
                                            # select adjust type (复权)
                                            html.Div(
                                                [
                                                    dbc.Label("复权类型:", className="text-light"),
                                                    dcc.Dropdown(
                                                        id="adjust-type-selector",
                                                        options=[
                                                            {"label": "不复权", "value": "none"},
                                                            {"label": "前复权", "value": AdjustType.qfq.value},
                                                            {"label": "后复权", "value": AdjustType.hfq.value}
                                                        ],
                                                        value="none",
                                                        clearable=False
                                                    ),
                                                ],
                                                className="mb-3",
                                            ),
                                            # select factor
                                            html.Div(
                                                [
                                                    dbc.Label("技术因子 (主/副图叠加):", className="text-light"),
                                                    dcc.Dropdown(
                                                        id="factor-selector",
                                                        placeholder="选择因子类",
                                                        options=[{"label": name, "value": name} for name in zvt_context.factor_cls_registry.keys()],
                                                        value="TechnicalFactor",
                                                    ),
                                                ],
                                                className="mb-3",
                                            ),

                                            # select additional data for sub-graph
                                            html.Div(
                                                [
                                                    dbc.Label("附加数据 (数据模型):", className="text-light"),
                                                    daq.BooleanSwitch(id="data-switch", on=True, label="仅看相关", labelPosition="top", style={"display": "inline-block", "float":"right"}),
                                                    dcc.Dropdown(id="data-selector", placeholder="选择副图数据模型"),
                                                ],
                                                className="mb-3",
                                            ),
                                            # select properties
                                            html.Div(
                                                [
                                                    dbc.Label("属性字段 (列):", className="text-light"),
                                                    dcc.Dropdown(id="schema-column-selector", placeholder="选择要展示的列", multi=True),
                                                ]
                                            ),
                                        ]
                                    )
                                ],
                                className="bg-secondary shadow"
                            )
                        ],
                        md=3, lg=2
                    ),
                    # Main Chart Area
                    dbc.Col(
                        [
                            html.Div(id="trader-details", className="mb-3"),
                            dcc.Loading(
                                type="default",
                                children=html.Div(id="factor-details")
                            )
                        ],
                        md=6, lg=6
                    ),
                    # 右侧列：上方股票信息(stock_meta)，下方点击K线时点估值
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader(html.H5("股票信息", className="m-0"), className="bg-dark text-light"),
                                    dbc.CardBody(html.Div(id="stock-meta-container", children=[html.P("选择实体标的后显示。", className="text-muted")])),
                                ],
                                className="mb-3 shadow",
                            ),
                            dbc.Card(
                                [
                                    dbc.CardHeader(html.H5("该时点估值", className="m-0"), className="bg-dark text-light"),
                                    dbc.CardBody(html.Div(id="valuation-at-point-container", children=[html.P("点击K线图上某点查看该日估值。", className="text-muted")])),
                                ],
                                className="shadow",
                            ),
                        ],
                        md=3, lg=4
                    ),
                ]
            )
        ]
    )
    return layout


@zvt_app.callback(
    Output("trader-selector", "options"),
    [Input("init-store", "data")]
)
def initialize_traders(_):
    ensure_traders_loaded()
    return [{"label": item, "value": i} for i, item in enumerate(trader_names)]


@zvt_app.callback(
    [
        Output("stats-chart-container", "children"),
        Output("signals-chart-container", "children"),
        Output("entity-type-selector", "options"),
        Output("entity-provider-selector", "options"),
        Output("entity-selector", "options"),
    ],
    [
        Input("trader-selector", "value"),
        Input("entity-type-selector", "value"),
        Input("entity-provider-selector", "value"),
    ],
)
def update_trader_details(trader_index, entity_type, entity_provider):
    ensure_traders_loaded()
    
    if trader_index is not None:
        # change entity_type options
        entity_type = traders[trader_index].entity_type
        if not entity_type:
            entity_type = "stock"
        entity_type_options = [{"label": entity_type, "value": entity_type}]

        # account stats
        account_stats_fig = get_account_stats_figure(account_stats_reader=account_readers[trader_index])
        if account_stats_fig is not None:
            account_stats_fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=10, r=10, t=10, b=10), height=300)
            account_stats_component = dcc.Graph(figure=account_stats_fig)
        else:
            account_stats_component = html.Div("未找到统计数据。", className="text-muted text-center mt-3")

        # trading signals (orders table)
        order_reader = order_readers[trader_index]
        if order_reader and pd_is_not_null(order_reader.data_df):
            df = order_reader.data_df.copy()
            cols = ['timestamp', 'entity_id', 'order_type', 'order_price', 'order_amount']
            disp_cols = [c for c in cols if c in df.columns]
            if disp_cols:
                disp_df = df.reset_index(drop=True)[disp_cols].sort_values('timestamp', ascending=False)
                signals_component = html.Div(
                    dbc.Table.from_dataframe(disp_df, striped=True, bordered=True, hover=True, size="sm", style={"color": "white"}), 
                    style={"maxHeight": "300px", "overflowY": "auto", "fontSize": "12px"}
                )
            else:
                signals_component = html.Div("未找到订单列数据", className="text-muted text-center mt-3")
        else:
            signals_component = html.Div("未找到订单数据", className="text-muted text-center mt-3")

        providers = zvt_context.tradable_schema_map.get(entity_type).providers
        entity_provider_options = [{"label": name, "value": name} for name in providers]

        # entities
        entity_ids = get_order_securities(trader_name=trader_names[trader_index])
        df = get_entities(
            provider=entity_provider,
            entity_type=entity_type,
            entity_ids=entity_ids,
            columns=["entity_id", "code", "name"],
            index="entity_id",
        )
        entity_options = [{"label": f'{entity_id}({entity["name"]})', "value": entity_id} for entity_id, entity in df.iterrows()]

        return account_stats_component, signals_component, entity_type_options, entity_provider_options, entity_options
    else:
        entity_type_options = [{"label": name, "value": name} for name in zvt_context.tradable_schema_map.keys()]
        
        providers = []
        if entity_type and entity_type in zvt_context.tradable_schema_map:
            providers = zvt_context.tradable_schema_map.get(entity_type).providers
        entity_provider_options = [{"label": name, "value": name} for name in providers]
        
        entity_options = []
        if entity_type and entity_provider:
            try:
                df = get_entities(
                    provider=entity_provider, entity_type=entity_type, columns=["entity_id", "code", "name"], index="entity_id"
                )
                if pd_is_not_null(df):
                    entity_options = [{"label": f'{entity["code"]} - {entity["name"]}', "value": entity_id} for entity_id, entity in df.iterrows()]
            except Exception as e:
                logger.error(f"Error fetching entities: {e}")
                
        empty_stats = html.Div(html.P("未选择交易员或未找到统计数据。", className="text-muted text-center mt-3"))
        empty_signals = html.Div(html.P("未选择交易员或未找到订单数据。", className="text-muted text-center mt-3"))
        return empty_stats, empty_signals, entity_type_options, entity_provider_options, entity_options


@zvt_app.callback(
    Output("data-selector", "options"), [Input("entity-type-selector", "value"), Input("data-switch", "on")]
)
def update_entity_selector(entity_type, related):
    if entity_type is not None:
        if related:
            schemas = zvt_context.entity_map_schemas.get(entity_type, [])
        else:
            schemas = zvt_context.schemas
        return [{"label": schema.__name__, "value": schema.__name__} for schema in schemas]
    raise dash.PreventUpdate()


@zvt_app.callback(Output("schema-column-selector", "options"), [Input("data-selector", "value")])
def update_column_selector(schema_name):
    if schema_name:
        schema = get_schema_by_name(name=schema_name)
        cols = get_schema_columns(schema=schema)
        # Filter out boring columns
        hidden_cols = {'id', 'entity_id', 'timestamp', 'provider', 'code', 'name'}
        return [{"label": col, "value": col} for col in cols if col not in hidden_cols]
    raise dash.PreventUpdate()


@zvt_app.callback(
    Output("stock-meta-container", "children"),
    [Input("entity-type-selector", "value"), Input("entity-provider-selector", "value"), Input("entity-selector", "value")],
)
def update_stock_meta(entity_type, entity_provider, entity):
    """右侧栏上方：展示 stock_meta 中该股票的信息。仅当实体类型为 stock 且有选中标的时展示。"""
    if entity_type != "stock" or not entity_provider or not entity:
        return html.P("选择股票标的后显示。", className="text-muted")
    try:
        meta_cols = ["code", "name", "list_date", "float_cap", "total_cap", "controlling_holder", "top_ten_ratio"]
        # index="entity_id" 时 get_entities 要求 entity_id 在 columns 中
        query_cols = ["entity_id"] + meta_cols
        df = get_entities(
            provider=entity_provider,
            entity_type="stock",
            entity_id=entity,
            columns=query_cols,
            return_type="df",
            index="entity_id",
        )
        if not pd_is_not_null(df) or df.empty:
            return html.P("未查到该股票 meta。", className="text-muted")
        row = df.iloc[0]
        rows = []
        for col in meta_cols:
            if col not in row or pd.isna(row.get(col)):
                continue
            val = row[col]
            if hasattr(val, "strftime"):
                val = val.strftime("%Y-%m-%d") if val else ""
            label = {"list_date": "上市日期", "float_cap": "流通市值", "total_cap": "总市值", "controlling_holder": "控股股东", "top_ten_ratio": "前十大股东占比"}.get(col, col)
            rows.append(html.Tr([html.Td(label, className="text-secondary"), html.Td(str(val), className="text-light")]))
        # 尝试补充 StockDetail：行业、概念等
        try:
            detail_df = StockDetail.query_data(provider=entity_provider, entity_id=entity, columns=["industries", "concept_indices", "main_business"], limit=1, return_type="df")
            if pd_is_not_null(detail_df) and not detail_df.empty:
                d = detail_df.iloc[0]
                if pd_is_not_null(d.get("industries")):
                    rows.append(html.Tr([html.Td("所属行业", className="text-secondary"), html.Td(str(d["industries"])[:200], className="text-light")]))
                if pd_is_not_null(d.get("concept_indices")):
                    rows.append(html.Tr([html.Td("概念板块", className="text-secondary"), html.Td(str(d["concept_indices"])[:200], className="text-light")]))
        except Exception:
            pass
        return dbc.Table(rows, size="sm", bordered=True, dark=True, style={"fontSize": "12px"})
    except Exception as e:
        logger.exception("update_stock_meta error")
        return html.P(f"加载失败: {e}", className="text-danger")


@zvt_app.callback(
    Output("factor-details", "children"),
    [
        Input("factor-selector", "value"),
        Input("entity-type-selector", "value"),
        Input("entity-provider-selector", "value"),
        Input("entity-selector", "value"),
        Input("levels-selector", "value"),
        Input("schema-column-selector", "value"),
        Input("adjust-type-selector", "value"),
    ],
    [State("trader-selector", "value"), State("data-selector", "value")],
)
def update_factor_details(factor, entity_type, entity_provider, entity, levels, columns, adjust_type, trader_index, schema_name):
    if factor and entity_type and entity_provider and entity and levels:
        ensure_traders_loaded()
        sub_df = None
        # add sub graph data based on selected schema
        if columns and schema_name:
            if type(columns) == str:
                columns = [columns]
            query_cols = columns + ["entity_id", "timestamp"]
            schema: Mixin = get_schema_by_name(name=schema_name)
            sub_df = schema.query_data(entity_id=entity, columns=query_cols)

        # add trading signals as annotation
        annotation_df = None
        if trader_index is not None:
            order_reader = order_readers[trader_index]
            annotation_df = order_reader.data_df.copy()
            if pd_is_not_null(annotation_df):
                annotation_df = annotation_df[annotation_df.entity_id == entity].copy()
                if not annotation_df.empty:
                    annotation_df["value"] = annotation_df["order_price"]
                    annotation_df["flag"] = annotation_df["order_type"].apply(lambda x: order_type_flag(x))
                    annotation_df["color"] = annotation_df["order_type"].apply(lambda x: order_type_color(x))

        if adjust_type == 'none':
            adj = 'none'
        else:
            try:
                adj = AdjustType(adjust_type) if adjust_type else None
            except ValueError:
                adj = None

        import inspect
        
        if type(levels) is list and len(levels) >= 2:
            levels.sort()
            drawers = []
            for level in levels:
                factor_cls = zvt_context.factor_cls_registry[factor]
                kwargs = {
                    "entity_schema": zvt_context.tradable_schema_map[entity_type],
                    "level": level,
                    "entity_ids": [entity]
                }
                sig = inspect.signature(factor_cls.__init__).parameters
                if "adjust_type" in sig:
                    kwargs["adjust_type"] = adj
                if "provider" in sig:
                    kwargs["provider"] = entity_provider
                if "entity_provider" in sig:
                    kwargs["entity_provider"] = entity_provider

                drawers.append(factor_cls(**kwargs).drawer())
            stacked = StackedDrawer(*drawers)
            fig = stacked.draw_kline(show=False, height=900)
            
        else:
            if type(levels) is list:
                level = levels[0]
            else:
                level = levels
            
            factor_cls = zvt_context.factor_cls_registry[factor]
            kwargs = {
                "entity_schema": zvt_context.tradable_schema_map[entity_type],
                "level": level,
                "entity_ids": [entity],
                "need_persist": False
            }
            sig = inspect.signature(factor_cls.__init__).parameters
            if "adjust_type" in sig:
                kwargs["adjust_type"] = adj
            if "provider" in sig:
                kwargs["provider"] = entity_provider
            if "entity_provider" in sig:
                kwargs["entity_provider"] = entity_provider
                
            drawer = factor_cls(**kwargs).drawer()
            
            if pd_is_not_null(sub_df):
                # Filter out columns we just fetched for the internal ZVT logic, pass only valid ones
                drawer.add_sub_df(sub_df)
            if pd_is_not_null(annotation_df):
                drawer.annotation_df = annotation_df

            fig = drawer.draw_kline(show=False, height=800)

        # Apply dark theme styling
        fig.update_layout(template="plotly_dark", paper_bgcolor='#222222', plot_bgcolor='#222222')
        # 固定 id 便于右侧栏“该时点估值”根据点击事件查询
        graph_config = {"displayModeBar": False, "displaylogo": False}
        return html.Div([
            dcc.Graph(id="factor-kline-graph", figure=fig, config=graph_config)
        ])

    raise dash.PreventUpdate()


@zvt_app.callback(
    Output("valuation-at-point-container", "children"),
    [Input("factor-kline-graph", "clickData")],
    [State("entity-type-selector", "value"), State("entity-selector", "value"), State("entity-provider-selector", "value")],
)
def update_valuation_at_point(click_data, entity_type, entity_id, entity_provider):
    """右侧栏下方：点击 K 线图某点时，展示该时点的估值指标（仅股票）。只查当日，不查最近一条。"""
    if entity_type != "stock" or not entity_id:
        return html.P("选择股票后，点击K线图上某点查看该日估值。", className="text-muted")
    if not click_data or "points" not in click_data or not click_data["points"]:
        return html.P("点击K线图上某点查看该日估值。", className="text-muted")
    try:
        pt = click_data["points"][0]
        x = pt.get("x")
        if x is None:
            return html.P("未获取到点击时间。", className="text-muted")
        if isinstance(x, (int, float)):
            ts = pd.Timestamp(x, unit="ms")
        else:
            ts = pd.Timestamp(str(x))
        # 取当日 0 点，并用当日全天时间范围查询（避免 DB 存了非 0 点时间导致查不到）
        ts = ts.normalize()
        end_of_day = ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        val_df = None
        # 优先用当前选的实体数据源，再试其他 valuation 的 provider
        providers = getattr(StockValuation, "providers", ["xysz", "joinquant"])
        if entity_provider and entity_provider in providers:
            providers = [entity_provider] + [p for p in providers if p != entity_provider]
        for prov in providers:
            try:
                val_df = StockValuation.query_data(
                    provider=prov,
                    entity_id=entity_id,
                    start_timestamp=ts,
                    end_timestamp=end_of_day,
                    return_type="df",
                )
                if pd_is_not_null(val_df) and not val_df.empty:
                    break
            except Exception:
                continue
        if not pd_is_not_null(val_df) or val_df.empty:
            return html.P(f"未找到 {ts.strftime('%Y-%m-%d')} 的估值数据。", className="text-muted")
        row = val_df.iloc[0]
        actual_ts = row.get("timestamp")
        if hasattr(actual_ts, "strftime"):
            date_str = actual_ts.strftime("%Y-%m-%d")
        else:
            date_str = pd.Timestamp(actual_ts).strftime("%Y-%m-%d") if actual_ts else ts.strftime("%Y-%m-%d")
        cols = ["pe", "pe_ttm", "pb", "ps", "pcf", "market_cap", "circulating_market_cap", "turnover_ratio", "capitalization", "circulating_cap"]
        labels = {"pe": "静态PE", "pe_ttm": "动态PE(TTM)", "pb": "市净率", "ps": "市销率", "pcf": "市现率", "market_cap": "总市值", "circulating_market_cap": "流通市值", "turnover_ratio": "换手率", "capitalization": "总股本", "circulating_cap": "流通股本"}

        def _scalar_not_null(v):
            """标量/单元格是否非空（pd_is_not_null 仅适用于 DataFrame/Series，对 numpy.float64 等会报 .empty 错误）"""
            if v is None:
                return False
            try:
                if pd.isna(v):
                    return False
            except (TypeError, ValueError):
                pass
            return True

        rows = [html.Tr([html.Td(labels.get(c, c), className="text-secondary"), html.Td(str(round(row[c], 4)) if isinstance(row.get(c), (int, float)) else str(row.get(c, "")), className="text-light")]) for c in cols if c in row and _scalar_not_null(row.get(c))]
        return html.Div([
            html.P(f"日期: {date_str}", className="text-info small mb-2"),
            dbc.Table(rows, size="sm", bordered=True, dark=True, style={"fontSize": "12px"}),
        ])
    except Exception as e:
        logger.exception("update_valuation_at_point error")
        return html.P(f"加载失败: {e}", className="text-danger")
