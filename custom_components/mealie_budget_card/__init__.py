from __future__ import annotations
import asyncio, csv, io, json, math, re, time
from xml.etree import ElementTree
from difflib import SequenceMatcher
from typing import Any
import aiohttp
import voluptuous as vol
from homeassistant.core import HomeAssistant
from homeassistant.components import websocket_api
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store
from .const import *

_CACHE: dict[str, tuple[float, Any]] = {}
_SALLING_LOCK = asyncio.Lock()
_LAST_SALLING_CALL = 0.0
MIN_CALL_INTERVAL = 1.25
STORAGE_VERSION = 1
STORAGE_KEY = "mealie_budget_card.rules"
UNITS = {"kg":1000.0,"g":1.0,"gram":1.0,"l":1000.0,"liter":1000.0,"dl":100.0,"ml":1.0,"stk":1.0,"stk.":1.0,"spsk":1.0,"spiseske":1.0,"tsk":1.0,"teske":1.0}
STOP = {"salt","peber","salt og peber","vand","olie","smør til stegning","krydderier","laurbærblad","laurbærblade"}
QUERY_ALIASES = {"bouillonterning":"grøntsagsbouillon","bouillonterninger":"grøntsagsbouillon","olivenolie":"rapsolie","cocktailtomat":"tomater","cocktailtomater":"tomater","parmesan":"parmesan ost","dijonsennep":"sennep","gulerod i tern":"gulerødder","gulerødder i tern":"gulerødder","løg i tern":"løg","mælk":"sødmælk","hvedemel":"hvedemel","kyllingebryst":"kyllingebryst","høns":"kylling"}
NEGATIVE_TOKENS = {"gulerødder":{"bolle","boller","solskinsboller"},"ketchup":{"pølsehorn","hotdog","hotdogs"},"rødløg":{"drueagurker","agurk"},"gulerod":{"bolle","boller","solskinsboller"},"mælk":{"ost","a38","yoghurt","skyr"},"salt":{"chips","snack"},"kyllingebryst":{"røget"},"asparges":{"friske"}}

async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    return True

async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    domain_data=hass.data.setdefault(DOMAIN,{})
    store=Store(hass,STORAGE_VERSION,f"{STORAGE_KEY}.{entry.entry_id}")
    saved=await store.async_load() or {"rules":{}}
    domain_data[entry.entry_id]={"entry":entry,"store":store,"rules":saved.get("rules",{})}
    if not domain_data.get("ws_registered"):
        websocket_api.async_register_command(hass, ws_analyse)
        websocket_api.async_register_command(hass, ws_test_salling)
        websocket_api.async_register_command(hass, ws_feed_info)
        websocket_api.async_register_command(hass, ws_diagnose_price)
        websocket_api.async_register_command(hass, ws_diagnose_feed)
        websocket_api.async_register_command(hass, ws_rule_candidates)
        websocket_api.async_register_command(hass, ws_save_rule)
        websocket_api.async_register_command(hass, ws_list_rules)
        domain_data["ws_registered"]=True
    return True

async def async_unload_entry(hass, entry):
    hass.data.get(DOMAIN,{}).pop(entry.entry_id,None)
    return True

def _entry(hass, entry_id):
    e=hass.config_entries.async_get_entry(entry_id)
    if not e or e.domain != DOMAIN: raise ValueError("Budget-integrationen blev ikke fundet")
    return e

async def _mealie(hass, service, data, response=True):
    return await hass.services.async_call("mealie",service,data,blocking=True,return_response=response)

def _recipe_items(response):
    if not isinstance(response,dict): return []
    r=response.get("recipes",response)
    return r.get("items",[]) if isinstance(r,dict) else []

def _unwrap_recipe(response):
    if not isinstance(response,dict): return {}
    for key in ("recipe","recipes"):
        v=response.get(key)
        if isinstance(v,dict) and "name" in v: return v
    return response

def _ingredients(recipe):
    raw=recipe.get("recipe_ingredient") or recipe.get("recipeIngredient") or recipe.get("ingredients") or []
    out=[]
    for x in raw:
        if isinstance(x,str): out.append({"display":x,"food":x})
        elif isinstance(x,dict):
            food=x.get("food") or {}; unit=x.get("unit") or {}
            out.append({"display":x.get("display") or x.get("note") or "", "food":food.get("name") if isinstance(food,dict) else str(food or ""), "quantity":x.get("quantity"), "unit":unit.get("name") if isinstance(unit,dict) else str(unit or "")})
    return out

def _parse(ing):
    text=(ing.get("display") or ing.get("food") or "").strip(); name=text
    q=ing.get("quantity"); unit=(ing.get("unit") or "").lower().strip()
    if q is None:
        m=re.match(r"^\s*(\d+(?:[.,]\d+)?)\s*(kg|g|gram|ml|dl|l|liter|stk\.?|spiseskeer?|spsk\.?|teskeer?|tsk\.?)?\s+(.+)$",text,re.I)
        if m: q=float(m.group(1).replace(",",".")); unit=(m.group(2) or "stk").lower(); name=m.group(3).strip()
    try: q=float(q) if q is not None else None
    except: q=None
    normalized=q*UNITS.get(unit,1.0) if q is not None else None
    cleaned=_clean_query(name)
    ignored=name.lower().strip() in STOP or cleaned in STOP
    return {"display":text,"query":cleaned,"quantity":q,"unit":unit,"normalized":normalized,"ignored":ignored}

def _pack_size(title,desc=""):
    t=f"{title} {desc}".lower().replace(",",".")
    for pat,mul in ((r"(\d+(?:\.\d+)?)\s*kg\b",1000),(r"(\d+(?:\.\d+)?)\s*g\b",1),(r"(\d+(?:\.\d+)?)\s*l\b",1000),(r"(\d+(?:\.\d+)?)\s*ml\b",1)):
        vals=re.findall(pat,t)
        if vals:return float(vals[-1])*mul
    return None

def _pick(d,*paths,default=None):
    for path in paths:
        cur=d
        try:
            for part in path.split("."):
                cur=cur.get(part) if isinstance(cur,dict) else None
            if cur is not None:return cur
        except: pass
    return default

def _flatten_feed(data):
    """Normalize the Recommendations feed without assuming one fixed wrapper."""
    candidates=[]
    if isinstance(data,list): candidates=data
    elif isinstance(data,dict):
        for key in ("items","products","recommendations","feed","data","results"):
            value=data.get(key)
            if isinstance(value,list): candidates=value; break
            if isinstance(value,dict):
                for sub in ("items","products","results"):
                    if isinstance(value.get(sub),list): candidates=value[sub]; break
                if candidates: break
    out=[]
    for raw in candidates:
        if not isinstance(raw,dict):continue
        product=raw.get("product") if isinstance(raw.get("product"),dict) else raw
        ean_value = _pick(product,"ean","gtin","barcode","product.ean",default="")
        if not ean_value:
            gtins = product.get("gtins")
            if isinstance(gtins, list) and gtins:
                ean_value = next((value for value in gtins if str(value).isdigit()), gtins[0])
        ean=str(ean_value or "")
        title=str(_pick(product,"title","name","description","productName",default="") or "")
        price=_pick(product,"price","salesPrice","currentPrice","pricing.price","prices.current",default=None)
        try: price=float(price) if price is not None else None
        except: price=None
        if title or ean: out.append({"ean":ean,"title":title,"price":price,"raw":product})
    return out

async def _get_json(hass,url,token,params=None,cache_key=None,ttl=43200):
    """Read Salling data with cache, pacing and Retry-After support."""
    global _LAST_SALLING_CALL
    now=time.time(); key=cache_key or f"{url}:{params or {}}"
    if key in _CACHE and now-_CACHE[key][0]<ttl:
        return _CACHE[key][1]

    session=async_get_clientsession(hass)
    last_error=None
    for attempt in range(1):
        async with _SALLING_LOCK:
            wait=max(0.0, MIN_CALL_INTERVAL-(time.monotonic()-_LAST_SALLING_CALL))
            if wait:
                await asyncio.sleep(wait)
            async with session.get(
                url,
                params=params,
                headers={"Authorization":f"Bearer {token}","Accept":"application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                _LAST_SALLING_CALL=time.monotonic()
                text=await r.text()
                if r.status==401:
                    raise ValueError("Salling Group afviste API-nøglen")
                if r.status==403:
                    raise ValueError(f"Salling Group gav 403 til endpointet {url}. Kontrollér scopes.")
                if r.status==429:
                    retry_header=r.headers.get("Retry-After")
                    try:
                        retry_after=float(retry_header) if retry_header else None
                    except (TypeError,ValueError):
                        retry_after=None
                    delay=retry_after if retry_after is not None else (5,12)[attempt]
                    last_error=f"Rate limit, nyt forsøg om {delay:g} sekunder"
                elif r.status>=400:
                    raise ValueError(f"Salling Group fejl {r.status}: {text[:300]}")
                else:
                    content_type=(r.headers.get("Content-Type") or "").lower()
                    clean=text.lstrip("\ufeff\n\r\t ")
                    data=None; parse_type="unknown"
                    try:
                        data=json.loads(clean); parse_type="json"
                    except Exception:
                        pass
                    if data is None and clean.startswith("<"):
                        try:
                            root=ElementTree.fromstring(clean); rows=[]
                            for node in root.iter():
                                children=list(node)
                                if not children: continue
                                row={}
                                for child in children:
                                    tag=child.tag.split("}")[-1]; value=(child.text or "").strip()
                                    if value: row[tag]=value
                                if row: rows.append(row)
                            data=rows; parse_type="xml"
                        except Exception:
                            pass
                    if data is None and clean:
                        try:
                            dialect=csv.Sniffer().sniff(clean[:2048],delimiters=",;\t|")
                            rows=list(csv.DictReader(io.StringIO(clean),dialect=dialect))
                            if rows: data=rows; parse_type="csv"
                        except Exception:
                            pass
                    if data is None:
                        preview=re.sub(r"\s+"," ",clean[:240])
                        raise ValueError(f"Salling-feed kunne ikke fortolkes. Content-Type: {content_type or 'ukendt'}. Svar starter med: {preview}")
                    wrapped={"_data":data,"_parse_type":parse_type,"_content_type":content_type}
                    _CACHE[key]=(time.time(),wrapped)
                    return wrapped
        if False:
            await asyncio.sleep(delay)

    raise ValueError(f"Salling Group har begrænset antallet af kald. {last_error or ''} Vent og prøv igen.")


def _store_items(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("stores", "items", "data", "results"):
            value = data.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                for sub in ("stores", "items", "results"):
                    if isinstance(value.get(sub), list):
                        return value[sub]
    return []


def _store_label(store):
    name = str(_pick(store, "name", "title", "brand", "type", default="") or "")
    address = str(_pick(store, "address.street", "address.streetName", "street", default="") or "")
    city = str(_pick(store, "address.city", "city", default="") or "")
    return " ".join(x for x in (name, address, city) if x).strip()


async def _find_store(hass, token, wanted=DEFAULT_STORE_NAME):
    wrapped = await _get_json(
        hass,
        STORES_URL,
        token,
        params={"per_page": 2000},
        cache_key="stores_v2",
        ttl=86400,
    )
    data = wrapped.get("_data") if isinstance(wrapped, dict) and "_data" in wrapped else wrapped
    stores = _store_items(data)
    wanted_tokens = _tokens(wanted)
    ranked = []
    for store in stores:
        if not isinstance(store, dict):
            continue
        label = _store_label(store)
        label_tokens = _tokens(label)
        overlap = len(wanted_tokens & label_tokens) / max(1, len(wanted_tokens))
        score = overlap * .85 + SequenceMatcher(None, wanted.lower(), label.lower()).ratio() * .15
        if "netto" in label.lower():
            score += .05
        ranked.append((score, store, label))
    if not ranked:
        raise ValueError("Stores API returnerede ingen butikker")
    ranked.sort(key=lambda item: item[0], reverse=True)
    score, store, label = ranked[0]
    store_id = _pick(store, "id", "storeId", "store_id", default=None)
    if not store_id:
        raise ValueError(f"Butikken {label or wanted} mangler storeId")
    return {"store_id": str(store_id), "label": label, "score": round(score, 3)}


async def _feed(hass,token):
    wrapped=await _get_json(hass,RECOMMENDATIONS_NETTO_URL,token,cache_key="nettoplus_feed_v3")
    data=wrapped.get("_data") if isinstance(wrapped,dict) and "_data" in wrapped else wrapped
    return _flatten_feed(data), wrapped

async def _ean(hass,token,ean,store_id):
    if not ean:return {}
    wrapped=await _get_json(
        hass,
        PRODUCT_EAN_URL.format(ean=ean),
        token,
        params={"storeId": store_id},
        cache_key=f"ean_v4:{store_id}:{ean}",
    )
    return wrapped.get("_data") if isinstance(wrapped,dict) and "_data" in wrapped else wrapped

def _clean_query(text):
    value=(text or "").lower().strip()
    value=re.sub(r"\b(i tern|delt i kvarte|i kvarte|på dåse|fra dåse|finthakket|hakket|revet|skåret|ristede|syltede|frisk|friske)\b"," ",value)
    value=re.sub(r"\s+"," ",value).strip()
    return QUERY_ALIASES.get(value,value)

def _is_bouillon(query):
    q=(query or "").lower()
    return "bouillon" in q or "fond" in q

def _tokens(s):
    return {x for x in re.findall(r"[a-zæøå0-9]+",(s or "").lower()) if len(x)>2}

def _score(query,title):
    qtext=_clean_query(query); ttext=(title or "").lower()
    q=_tokens(qtext); t=_tokens(ttext)
    if not q or not (q & t):
        return 0.0
    for anchor, blocked in NEGATIVE_TOKENS.items():
        if anchor in qtext and blocked & t:
            return 0.0
    overlap=len(q&t)/max(1,len(q))
    seq=SequenceMatcher(None,qtext,ttext).ratio()
    return round(overlap*.85+seq*.15,4)

def _best_feed_match(parsed,feed):
    ranked=[]
    for p in feed:
        score=_score(parsed["query"],p["title"])
        if score>=0.48: ranked.append((score,p))
    if not ranked:return None
    ranked.sort(key=lambda x:x[0],reverse=True)
    return {**ranked[0][1],"score":ranked[0][0]}

def _normalize_product(match,details):
    d = details if isinstance(details, dict) else {}
    if isinstance(d.get("product"), dict):
        d = d["product"]

    instore = d.get("instore") if isinstance(d.get("instore"), dict) else {}
    webshop = d.get("webshop") if isinstance(d.get("webshop"), dict) else {}

    title = str(
        _pick(
            d,
            "instore.name",
            "webshop.name",
            "title",
            "name",
            "description",
            "productName",
            default=match.get("title", ""),
        )
        or ""
    )

    price = _pick(
        d,
        "instore.price",
        "webshop.price",
        "price",
        "salesPrice",
        "currentPrice",
        "pricing.price",
        "prices.current",
        default=match.get("price"),
    )
    unit_price = _pick(d, "instore.unitPrice", "webshop.unitPrice", default=None)
    contents = _pick(d, "instore.contents", "webshop.contents", default=None)
    contents_unit = str(
        _pick(d, "instore.contentsUnit", "webshop.contentsUnit", default="") or ""
    ).lower()
    sale_unit = str(_pick(d, "instore.unit", "webshop.unit", default="") or "").lower()

    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    try:
        unit_price = float(unit_price) if unit_price is not None else None
    except (TypeError, ValueError):
        unit_price = None
    try:
        contents = float(contents) if contents is not None else None
    except (TypeError, ValueError):
        contents = None

    size = None
    if contents is not None:
        if contents_unit in UNITS:
            size = contents * UNITS[contents_unit]
        elif contents_unit in ("stk", "stk."):
            size = contents
    if size is None:
        size = _pack_size(title, str(d.get("description", "")))

    return {
        "ean": match.get("ean"),
        "title": title,
        "price": price,
        "unit_price": unit_price,
        "size": size,
        "contents": contents,
        "contents_unit": contents_unit,
        "sale_unit": sale_unit,
        "score": match.get("score"),
        "raw_price_found": price is not None,
        "price_source": "instore" if instore.get("price") is not None else ("webshop" if webshop.get("price") is not None else "fallback"),
    }

def _calculate(parsed,product):
    price=product.get("price");size=product.get("size")
    if not isinstance(price,(int,float)):
        return {**product,"purchase_price":None,"consumption_price":None,"packs":None,"quality":"matched_no_price"}
    if _is_bouillon(parsed.get("query")):
        return {**product,"packs":1,"purchase_price":round(price,2),"consumption_price":round(price,2),"quality":"estimated_bouillon"}
    if parsed.get("normalized") and size:
        packs=max(1,math.ceil(parsed["normalized"]/size))
        return {**product,"packs":packs,"purchase_price":round(packs*price,2),"consumption_price":round(parsed["normalized"]/size*price,2),"quality":"precise"}
    return {**product,"packs":1,"purchase_price":round(price,2),"consumption_price":round(price,2),"quality":"estimated"}

@websocket_api.websocket_command({vol.Required("type"):"mealie_budget_card/test_salling",vol.Required("entry_id"):str})
@websocket_api.async_response
async def ws_test_salling(hass,connection,msg):
    try:
        e=_entry(hass,msg["entry_id"]); feed,raw=await _feed(hass,e.data[CONF_SALLING_TOKEN])
        connection.send_result(msg["id"],{"ok":True,"count":len(feed),"endpoint":RECOMMENDATIONS_NETTO_URL,"ean_count":sum(1 for x in feed if x.get("ean")),"parse_type":raw.get("_parse_type","unknown") if isinstance(raw,dict) else "unknown","content_type":raw.get("_content_type","") if isinstance(raw,dict) else ""})
    except Exception as err:connection.send_error(msg["id"],"test_failed",str(err))

@websocket_api.websocket_command({vol.Required("type"):"mealie_budget_card/feed_info",vol.Required("entry_id"):str})
@websocket_api.async_response
async def ws_feed_info(hass,connection,msg):
    try:
        e=_entry(hass,msg["entry_id"]);feed,raw=await _feed(hass,e.data[CONF_SALLING_TOKEN])
        connection.send_result(msg["id"],{"count":len(feed),"sample":feed[:3],"raw_type":type(raw).__name__,"parse_type":raw.get("_parse_type","unknown") if isinstance(raw,dict) else "unknown","content_type":raw.get("_content_type","") if isinstance(raw,dict) else ""})
    except Exception as err:connection.send_error(msg["id"],"feed_failed",str(err))



def _safe_shape(value, depth=0):
    """Return a secret-safe structural preview with numeric values preserved."""
    if depth > 5:
        return "..."
    if isinstance(value, dict):
        blocked = {"token", "authorization", "api_key", "apikey", "key", "secret"}
        return {
            str(k): ("[skjult]" if str(k).lower() in blocked else _safe_shape(v, depth + 1))
            for k, v in list(value.items())[:80]
        }
    if isinstance(value, list):
        return [_safe_shape(v, depth + 1) for v in value[:3]]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    text = str(value)
    return text[:180]


def _numeric_paths(value, prefix="", depth=0):
    found = []
    if depth > 6:
        return found
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            found.extend(_numeric_paths(child, path, depth + 1))
    elif isinstance(value, list):
        for idx, child in enumerate(value[:5]):
            found.extend(_numeric_paths(child, f"{prefix}[{idx}]", depth + 1))
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        found.append({"path": prefix, "value": value})
    return found[:120]




@websocket_api.websocket_command({
    vol.Required("type"): "mealie_budget_card/diagnose_feed",
    vol.Required("entry_id"): str,
})
@websocket_api.async_response
async def ws_diagnose_feed(hass, connection, msg):
    """Return a safe sample of the raw Netto recommendations feed."""
    try:
        entry = _entry(hass, msg["entry_id"])
        token = entry.data[CONF_SALLING_TOKEN]
        feed, wrapped = await _feed(hass, token)
        raw = wrapped.get("_data") if isinstance(wrapped, dict) else wrapped
        connection.send_result(msg["id"], {
            "normalized_count": len(feed),
            "normalized_sample": _safe_shape(feed[:3]),
            "raw_type": type(raw).__name__,
            "parse_type": wrapped.get("_parse_type", "unknown") if isinstance(wrapped, dict) else "unknown",
            "content_type": wrapped.get("_content_type", "") if isinstance(wrapped, dict) else "",
            "raw_sample": _safe_shape(raw),
        })
    except Exception as err:
        connection.send_error(msg["id"], "diagnose_feed_failed", str(err))

@websocket_api.websocket_command({
    vol.Required("type"): "mealie_budget_card/diagnose_price",
    vol.Required("entry_id"): str,
})
@websocket_api.async_response
async def ws_diagnose_price(hass, connection, msg):
    try:
        entry = _entry(hass, msg["entry_id"])
        token = entry.data[CONF_SALLING_TOKEN]
        feed, raw_feed = await _feed(hass, token)
        store = await _find_store(hass, token)
        candidate = next((item for item in feed if item.get("ean")), None)
        if not candidate:
            raise ValueError("Netto-feedet indeholder ingen produkter med EAN")
        details = await _ean(hass, token, candidate["ean"], store["store_id"])
        connection.send_result(msg["id"], {
            "ean": candidate.get("ean"),
            "feed_title": candidate.get("title"),
            "feed_price": candidate.get("price"),
            "store_id": store.get("store_id"),
            "store_label": store.get("label"),
            "store_match_score": store.get("score"),
            "details_type": type(details).__name__,
            "numeric_fields": _numeric_paths(details),
            "structure": _safe_shape(details),
        })
    except Exception as err:
        connection.send_error(msg["id"], "diagnose_failed", str(err))


def _rule_key(query):
    return _clean_query(query).strip().lower()


def _runtime(hass, entry_id):
    value=hass.data.get(DOMAIN,{}).get(entry_id)
    if not isinstance(value,dict) or "rules" not in value:
        raise ValueError("Budget-integrationens regellager er ikke indlæst")
    return value


@websocket_api.websocket_command({
    vol.Required("type"):"mealie_budget_card/rule_candidates",
    vol.Required("entry_id"):str,
    vol.Required("query"):str,
    vol.Optional("search"):str,
})
@websocket_api.async_response
async def ws_rule_candidates(hass,connection,msg):
    try:
        entry=_entry(hass,msg["entry_id"]); token=entry.data[CONF_SALLING_TOKEN]
        feed,_=await _feed(hass,token)
        search_text=_clean_query(msg.get("search") or msg["query"])
        search_tokens=_tokens(search_text)
        ranked=[]
        for product in feed:
            title=product.get("title","")
            title_tokens=_tokens(title)
            if msg.get("search"):
                overlap=len(search_tokens & title_tokens)/max(1,len(search_tokens))
                sequence=SequenceMatcher(None,search_text.lower(),title.lower()).ratio()
                score=round(overlap*.75+sequence*.25,4)
                if not (search_tokens & title_tokens) and search_text.lower() not in title.lower():
                    continue
            else:
                score=_score(search_text,title)
            if score>0:
                ranked.append({"ean":product.get("ean"),"title":title,"score":score})
        ranked.sort(key=lambda value:value["score"],reverse=True)
        runtime=_runtime(hass,msg["entry_id"]); rule=runtime["rules"].get(_rule_key(msg["query"]))
        connection.send_result(msg["id"],{"query":msg["query"],"search":search_text,"key":_rule_key(msg["query"]),"candidates":ranked[:20],"rule":rule})
    except Exception as err:
        connection.send_error(msg["id"],"candidates_failed",str(err))


@websocket_api.websocket_command({
    vol.Required("type"):"mealie_budget_card/save_rule",
    vol.Required("entry_id"):str,
    vol.Required("query"):str,
    vol.Required("mode"):vol.In(["product","ignore","delete"]),
    vol.Optional("ean"):str,
    vol.Optional("title"):str,
})
@websocket_api.async_response
async def ws_save_rule(hass,connection,msg):
    try:
        runtime=_runtime(hass,msg["entry_id"]); key=_rule_key(msg["query"])
        if msg["mode"]=="delete":
            runtime["rules"].pop(key,None)
        elif msg["mode"]=="ignore":
            runtime["rules"][key]={"mode":"ignore","query":msg["query"]}
        else:
            if not msg.get("ean"):
                raise ValueError("EAN mangler")
            runtime["rules"][key]={"mode":"product","query":msg["query"],"ean":msg["ean"],"title":msg.get("title","")}
        await runtime["store"].async_save({"rules":runtime["rules"]})
        connection.send_result(msg["id"],{"ok":True,"key":key,"rule":runtime["rules"].get(key),"count":len(runtime["rules"])})
    except Exception as err:
        connection.send_error(msg["id"],"save_rule_failed",str(err))


@websocket_api.websocket_command({vol.Required("type"):"mealie_budget_card/list_rules",vol.Required("entry_id"):str})
@websocket_api.async_response
async def ws_list_rules(hass,connection,msg):
    try:
        rules=_runtime(hass,msg["entry_id"])["rules"]
        connection.send_result(msg["id"],{"rules":rules,"count":len(rules)})
    except Exception as err:
        connection.send_error(msg["id"],"list_rules_failed",str(err))


@websocket_api.websocket_command({vol.Required("type"):"mealie_budget_card/analyse",vol.Required("entry_id"):str})
@websocket_api.async_response
async def ws_analyse(hass,connection,msg):
    try:
        e=_entry(hass,msg["entry_id"]);d=e.data;token=d[CONF_SALLING_TOKEN];mealie_id=d[CONF_MEALIE_CONFIG_ENTRY_ID]
        feed,_=await _feed(hass,token)
        store=await _find_store(hass,token)
        rules=_runtime(hass,msg["entry_id"])["rules"]
        ean_lookups=0
        ean_limit=5
        resp=await _mealie(hass,"get_recipes",{"config_entry_id":mealie_id,"search_terms":d.get(CONF_CATEGORY,DEFAULT_CATEGORY),"result_limit":d.get(CONF_MAX_RECIPES,10)})
        summaries=_recipe_items(resp);results=[]
        for summary in summaries:
            rid=summary.get("recipe_id") or summary.get("id") or summary.get("slug")
            full=_unwrap_recipe(await _mealie(hass,"get_recipe",{"config_entry_id":mealie_id,"recipe_id":rid}))
            servings=full.get("recipe_servings") or full.get("recipeServings") or d.get(CONF_SERVINGS,4)
            try:scale=d.get(CONF_SERVINGS,4)/float(servings or d.get(CONF_SERVINGS,4))
            except:scale=1
            matches=[];calculable=0;matched=0;priced=0;purchase=0;consumption=0
            for ing0 in _ingredients(full):
                p=_parse(ing0)
                rule=rules.get(_rule_key(p["query"]))
                if p["ignored"] or (rule and rule.get("mode")=="ignore"):
                    matches.append({**p,"status":"ignored","rule":rule});continue
                if p["quantity"] is not None:p["quantity"]*=scale;p["normalized"]=(p["normalized"]*scale if p["normalized"] else None);calculable+=1
                if rule and rule.get("mode")=="product":
                    fm=next((item for item in feed if str(item.get("ean"))==str(rule.get("ean"))),None)
                    if fm: fm={**fm,"score":1.0,"manual":True}
                else:
                    fm=_best_feed_match(p,feed)
                chosen=None
                if fm:
                    matched+=1
                    if fm.get("ean") and ean_lookups < ean_limit:
                        try:
                            details=await _ean(hass,token,fm.get("ean"),store["store_id"])
                            ean_lookups += 1
                            chosen=_calculate(p,_normalize_product(fm,details))
                        except ValueError as err:
                            if "begrænset" in str(err) or "Rate limit" in str(err):
                                chosen={"ean":fm.get("ean"),"title":fm.get("title"),"score":fm.get("score"),"purchase_price":None,"consumption_price":None,"quality":"deferred_rate_limit"}
                                ean_lookups=ean_limit
                            else:
                                raise
                    elif fm.get("ean"):
                        chosen={"ean":fm.get("ean"),"title":fm.get("title"),"score":fm.get("score"),"purchase_price":None,"consumption_price":None,"quality":"deferred_limit"}
                    if chosen.get("purchase_price") is not None:
                        priced+=1;purchase+=chosen["purchase_price"];consumption+=chosen["consumption_price"]
                matches.append({**p,"status":"priced" if chosen and chosen.get("purchase_price") is not None else ("matched_no_price" if chosen else "unmatched"),"product":chosen,"rule":rule,"manual":bool(fm and fm.get("manual"))})
                await asyncio.sleep(.05)
            coverage=round(priced/calculable*100) if calculable else 0
            match_coverage=round(matched/calculable*100) if calculable else 0
            results.append({"recipe_id":rid,"name":full.get("name") or summary.get("name"),"slug":full.get("slug") or summary.get("slug"),"purchase_price":round(purchase,2),"consumption_price":round(consumption,2),"per_person":round(purchase/d.get(CONF_SERVINGS,4),2),"coverage":coverage,"match_coverage":match_coverage,"unmatched":sum(1 for x in matches if x["status"]=="unmatched"),"matched_without_price":sum(1 for x in matches if x["status"]=="matched_no_price"),"matches":matches})
        results.sort(key=lambda x:(-x["coverage"],x["purchase_price"]))
        connection.send_result(msg["id"],{"recipes":results,"feed_products":len(feed),"source":"Salling Recommendations nettoplus feed + Products EAN API","store":store,"rule_count":len(rules),"ean_lookups":ean_lookups,"ean_limit":ean_limit,"partial":ean_lookups>=ean_limit,"analysis_only":True})
    except Exception as err:connection.send_error(msg["id"],"analysis_failed",str(err))
