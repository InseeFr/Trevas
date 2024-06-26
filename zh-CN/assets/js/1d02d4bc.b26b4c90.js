"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[1758],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>f});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),u=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},d=function(e){var t=u(e.components);return a.createElement(s.Provider,{value:t},e.children)},c="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,d=i(e,["components","mdxType","originalType","parentName"]),c=u(n),m=r,f=c["".concat(s,".").concat(m)]||c[m]||p[m]||o;return n?a.createElement(f,l(l({ref:t},d),{},{components:n})):a.createElement(f,l({ref:t},d))}));function f(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,l=new Array(o);l[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[c]="string"==typeof e?e:r,l[1]=i;for(var u=2;u<o;u++)l[u]=n[u];return a.createElement.apply(null,l)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},70945:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>d,contentTitle:()=>s,default:()=>m,frontMatter:()=>i,metadata:()=>u,toc:()=>c});var a=n(87462),r=(n(67294),n(3905)),o=n(44996),l=n(50941);const i={slug:"/trevas-sdmx",title:"Trevas - SDMX",authors:["nicolas"],tags:["Trevas","SDMX"]},s=void 0,u={permalink:"/Trevas/zh-CN/blog/trevas-sdmx",source:"@site/blog/2024-06-25-trevas-sdmx.mdx",title:"Trevas - SDMX",description:"News",date:"2024-06-25T00:00:00.000Z",formattedDate:"2024\u5e746\u670825\u65e5",tags:[{label:"Trevas",permalink:"/Trevas/zh-CN/blog/tags/trevas"},{label:"SDMX",permalink:"/Trevas/zh-CN/blog/tags/sdmx"}],readingTime:2.44,hasTruncateMarker:!1,authors:[{name:"Nicolas Laval",link:"https://github.com/NicoLaval",title:"Making Sense - Developer",image:"/img/profile_pic_nicolas_laval.jpg",key:"nicolas"}],frontMatter:{slug:"/trevas-sdmx",title:"Trevas - SDMX",authors:["nicolas"],tags:["Trevas","SDMX"]},nextItem:{title:"Trevas - Temporal operators",permalink:"/Trevas/zh-CN/blog/trevas-temporal-operators"}},d={authorsImageUrls:[void 0]},c=[{value:"News",id:"news",level:3},{value:"Overview",id:"overview",level:4},{value:"Tools available",id:"tools-available",level:4},{value:"<code>buildStructureFromSDMX3</code> utility",id:"buildstructurefromsdmx3-utility",level:4},{value:"<code>SDMXVTLWorkflow</code> object",id:"sdmxvtlworkflow-object",level:4},{value:"SDMXVTLWorkflow <code>run</code> function - Preview mode",id:"sdmxvtlworkflow-run-function---preview-mode",level:4},{value:"SDMXVTLWorkflow <code>run</code> function",id:"sdmxvtlworkflow-run-function",level:4},{value:"SDMXVTLWorkflow <code>getTransformationsVTL</code> function",id:"sdmxvtlworkflow-gettransformationsvtl-function",level:4},{value:"SDMXVTLWorkflow <code>getRulesetsVTL</code> function",id:"sdmxvtlworkflow-getrulesetsvtl-function",level:4}],p={toc:c};function m(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h3",{id:"news"},"News"),(0,r.kt)("p",null,"Trevas 1.4.1 introduces the VTL SDMX module."),(0,r.kt)("p",null,"This module enables to consume SDMX metadata sources to instantiate Trevas DataStructures and Datasets."),(0,r.kt)("p",null,"It also allows to execute the VTL TransformationSchemes to obtain the resulting persistent datasets."),(0,r.kt)("h4",{id:"overview"},"Overview"),(0,r.kt)("div",{style:{textAlign:"center"}},(0,r.kt)(l.Z,{alt:"VTL SDMX Diagram",sources:{light:(0,o.Z)("/img/vtl-sdmx-light.svg"),dark:(0,o.Z)("/img/vtl-sdmx-dark.svg")},mdxType:"ThemedImage"})),(0,r.kt)("p",null,"Trevas supports the above SDMX message elements. Only the VtlMappingSchemes attribute is optional."),(0,r.kt)("p",null,"The elements in box 1 are used to produce Trevas DataStructures, filling VTL components attributes name, role, type, nullable and valuedomain."),(0,r.kt)("p",null,"The elements in box 2 are used to generate the VTL code (rulesets & transformations)."),(0,r.kt)("h4",{id:"tools-available"},"Tools available"),(0,r.kt)("h4",{id:"buildstructurefromsdmx3-utility"},(0,r.kt)("inlineCode",{parentName:"h4"},"buildStructureFromSDMX3")," utility"),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"TrevasSDMXUtils.buildStructureFromSDMX3")," allows to obtain a Trevas DataStructure."),(0,r.kt)("p",null,"Providing corresponding data, you can build a Trevas Dataset."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'Structured.DataStructure structure = TrevasSDMXUtils.buildStructureFromSDMX3("path/sdmx_file.xml", "STRUCT_ID");\n\nSparkDataset ds = new SparkDataset(\n        spark.read()\n                .option("header", "true")\n                .option("delimiter", ";")\n                .option("quote", "\\"")\n                .csv("path"),\n        structure\n);\n')),(0,r.kt)("h4",{id:"sdmxvtlworkflow-object"},(0,r.kt)("inlineCode",{parentName:"h4"},"SDMXVTLWorkflow")," object"),(0,r.kt)("p",null,"The ",(0,r.kt)("inlineCode",{parentName:"p"},"SDMXVTLWorkflow")," constructor takes 3 arguments:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"a ",(0,r.kt)("inlineCode",{parentName:"li"},"ScriptEngine")," (Trevas or another)"),(0,r.kt)("li",{parentName:"ul"},"a ",(0,r.kt)("inlineCode",{parentName:"li"},"ReadableDataLocation")," to handle an SDMX message"),(0,r.kt)("li",{parentName:"ul"},"a map of names / Datasets")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'SparkSession.builder()\n                .appName("test")\n                .master("local")\n                .getOrCreate();\n\nScriptEngineManager mgr = new ScriptEngineManager();\nScriptEngine engine = mgr.getEngineByExtension("vtl");\nengine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");\n\nReadableDataLocation rdl = new ReadableDataLocationTmp("src/test/resources/DSD_BPE_CENSUS.xml");\n\nSDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, Map.of());\n')),(0,r.kt)("p",null,"This object then allows you to activate the following 3 functions."),(0,r.kt)("h4",{id:"sdmxvtlworkflow-run-function---preview-mode"},"SDMXVTLWorkflow ",(0,r.kt)("inlineCode",{parentName:"h4"},"run")," function - Preview mode"),(0,r.kt)("p",null,"The ",(0,r.kt)("inlineCode",{parentName:"p"},"run")," function can easily be called in a preview mode, without attached data."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'ScriptEngineManager mgr = new ScriptEngineManager();\nScriptEngine engine = mgr.getEngineByExtension("vtl");\nengine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");\n\nReadableDataLocation rdl = new ReadableDataLocationTmp("src/test/resources/DSD_BPE_CENSUS.xml");\n\nSDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, Map.of());\n\n// instead of using TrevasSDMXUtils.buildStructureFromSDMX3 and data sources\n// to build Trevas Datasets, sdmxVtlWorkflow.getEmptyDatasets()\n// will handle SDMX message structures to produce Trevas Datasets\n// with metadata defined in this message, and adding empty data\nMap<String, Dataset> emptyDatasets = sdmxVtlWorkflow.getEmptyDatasets();\nengine.getBindings(ScriptContext.ENGINE_SCOPE).putAll(emptyDatasets);\n\nMap<String, PersistentDataset> result = sdmxVtlWorkflow.run();\n')),(0,r.kt)("p",null,"The preview mode allows to check the conformity of the SDMX file and the metadata of the output datasets."),(0,r.kt)("h4",{id:"sdmxvtlworkflow-run-function"},"SDMXVTLWorkflow ",(0,r.kt)("inlineCode",{parentName:"h4"},"run")," function"),(0,r.kt)("p",null,"Once an ",(0,r.kt)("inlineCode",{parentName:"p"},"SDMXVTLWorkflow")," is built, it is easy to run the VTL validations and transformations defined in the SDMX file."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'Structured.DataStructure structure = TrevasSDMXUtils.buildStructureFromSDMX3("path/sdmx_file.xml", "ds1");\n\nSparkDataset ds1 = new SparkDataset(\n        spark.read()\n                .option("header", "true")\n                .option("delimiter", ";")\n                .option("quote", "\\"")\n                .csv("path/data.csv"),\n        structure\n);\n\nScriptEngineManager mgr = new ScriptEngineManager();\nScriptEngine engine = mgr.getEngineByExtension("vtl");\nengine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");\n\nMap<String, Dataset> inputs = Map.of("ds1", ds1);\n\nReadableDataLocation rdl = new ReadableDataLocationTmp("path/sdmx_file.xml");\n\nSDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, inputs);\n\nMap<String, PersistentDataset> bindings = sdmxVtlWorkflow.run();\n')),(0,r.kt)("p",null,"As a result, one will receive all the dataset defined as persistent in the ",(0,r.kt)("inlineCode",{parentName:"p"},"TransformationSchemes")," definition."),(0,r.kt)("h4",{id:"sdmxvtlworkflow-gettransformationsvtl-function"},"SDMXVTLWorkflow ",(0,r.kt)("inlineCode",{parentName:"h4"},"getTransformationsVTL")," function"),(0,r.kt)("p",null,"Gets the VTL code corresponding to the SDMX TransformationSchemes definition."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"SDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, Map.of());\nString vtl = sdmxVtlWorkflow.getTransformationsVTL();\n")),(0,r.kt)("h4",{id:"sdmxvtlworkflow-getrulesetsvtl-function"},"SDMXVTLWorkflow ",(0,r.kt)("inlineCode",{parentName:"h4"},"getRulesetsVTL")," function"),(0,r.kt)("p",null,"Gets the VTL code corresponding to the SDMX TransformationSchemes definition."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},"SDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, Map.of());\nString dprs = sdmxVtlWorkflow.getRulesetsVTL();\n")))}m.isMDXComponent=!0}}]);