"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[294],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>m});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function s(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?s(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):s(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},s=Object.keys(e);for(r=0;r<s.length;r++)n=s[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var s=Object.getOwnPropertySymbols(e);for(r=0;r<s.length;r++)n=s[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var l=r.createContext({}),p=function(e){var t=r.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},c=function(e){var t=p(e.components);return r.createElement(l.Provider,{value:t},e.children)},u="mdxType",v={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,s=e.originalType,l=e.parentName,c=o(e,["components","mdxType","originalType","parentName"]),u=p(n),d=a,m=u["".concat(l,".").concat(d)]||u[d]||v[d]||s;return n?r.createElement(m,i(i({ref:t},c),{},{components:n})):r.createElement(m,i({ref:t},c))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var s=n.length,i=new Array(s);i[0]=d;var o={};for(var l in t)hasOwnProperty.call(t,l)&&(o[l]=t[l]);o.originalType=e,o[u]="string"==typeof e?e:a,i[1]=o;for(var p=2;p<s;p++)i[p]=n[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},974:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>u,frontMatter:()=>s,metadata:()=>o,toc:()=>p});var r=n(7462),a=(n(7294),n(3905));const s={slug:"/trevas-persistent-assignments",title:"Trevas - Persistent assignments",authors:["nicolas"],tags:["Trevas"]},i=void 0,o={permalink:"/Trevas/zh-CN/blog/trevas-persistent-assignments",source:"@site/blog/2023-11-22-trevas-persistent-assignment.mdx",title:"Trevas - Persistent assignments",description:"News",date:"2023-11-22T00:00:00.000Z",formattedDate:"2023\u5e7411\u670822\u65e5",tags:[{label:"Trevas",permalink:"/Trevas/zh-CN/blog/tags/trevas"}],readingTime:.41,hasTruncateMarker:!1,authors:[{name:"Nicolas Laval",link:"https://github.com/NicoLaval",title:"Making Sense - Developer",image:"profile_pic_nicolas_laval.jpg",key:"nicolas"}],frontMatter:{slug:"/trevas-persistent-assignments",title:"Trevas - Persistent assignments",authors:["nicolas"],tags:["Trevas"]},prevItem:{title:"Trevas - Java 17",permalink:"/Trevas/zh-CN/blog/trevas-java-17"},nextItem:{title:"Trevas - check_hierarchy",permalink:"/Trevas/zh-CN/blog/trevas-check_hierarchy"}},l={authorsImageUrls:[void 0]},p=[{value:"News",id:"news",level:3},{value:"Handle <code>PersistentDataset</code>",id:"handle-persistentdataset",level:3}],c={toc:p};function u(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,r.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h3",{id:"news"},"News"),(0,a.kt)("p",null,"Trevas 1.2.0 includes the persistent assignment support: ",(0,a.kt)("inlineCode",{parentName:"p"},"ds1 <- ds;"),"."),(0,a.kt)("p",null,"In Trevas, persistent datasets are represented as ",(0,a.kt)("inlineCode",{parentName:"p"},"PersistentDataset"),"."),(0,a.kt)("h3",{id:"handle-persistentdataset"},"Handle ",(0,a.kt)("inlineCode",{parentName:"h3"},"PersistentDataset")),(0,a.kt)("p",null,"Trevas datasets are represented as ",(0,a.kt)("inlineCode",{parentName:"p"},"Dataset"),"."),(0,a.kt)("p",null,"After running the Trevas engine, you can use persistent datasets with something like:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"Bindings engineBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);\nengineBindings.forEach((k, v) -> {\n    if (v instanceof PersistentDataset) {\n        fr.insee.vtl.model.Dataset ds = ((PersistentDataset) v).getDelegate();\n        if (ds instanceof SparkDataset) {\n            Dataset<Row> sparkDs = ((SparkDataset) ds).getSparkDataset();\n            // Do what you want with sparkDs\n        }\n    }\n});\n")))}u.isMDXComponent=!0}}]);