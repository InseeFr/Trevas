"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[7583],{3905:(e,t,r)=>{r.d(t,{Zo:()=>c,kt:()=>v});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var i=n.createContext({}),d=function(e){var t=n.useContext(i),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},c=function(e){var t=d(e.components);return n.createElement(i.Provider,{value:t},e.children)},u="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,i=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),u=d(r),m=a,v=u["".concat(i,".").concat(m)]||u[m]||p[m]||o;return r?n.createElement(v,l(l({ref:t},c),{},{components:r})):n.createElement(v,l({ref:t},c))}));function v(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,l=new Array(o);l[0]=m;var s={};for(var i in t)hasOwnProperty.call(t,i)&&(s[i]=t[i]);s.originalType=e,s[u]="string"==typeof e?e:a,l[1]=s;for(var d=2;d<o;d++)l[d]=r[d];return n.createElement.apply(null,l)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},272:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>i,contentTitle:()=>l,default:()=>u,frontMatter:()=>o,metadata:()=>s,toc:()=>d});var n=r(7462),a=(r(7294),r(3905));const o={id:"json",title:"Mode de base - Source JSON",sidebar_label:"JSON",slug:"/developer-guide/basic-mode/data-sources/json",custom_edit_url:null},l=void 0,s={unversionedId:"developer-guide/basic-mode/data-sources/json",id:"developer-guide/basic-mode/data-sources/json",title:"Mode de base - Source JSON",description:"Importer le module jackson de Trevas",source:"@site/i18n/fr/docusaurus-plugin-content-docs/current/developer-guide/basic-mode/data-sources/json.mdx",sourceDirName:"developer-guide/basic-mode/data-sources",slug:"/developer-guide/basic-mode/data-sources/json",permalink:"/Trevas/fr/developer-guide/basic-mode/data-sources/json",draft:!1,editUrl:null,tags:[],version:"current",lastUpdatedAt:1694334690,formattedLastUpdatedAt:"10 sept. 2023",frontMatter:{id:"json",title:"Mode de base - Source JSON",sidebar_label:"JSON",slug:"/developer-guide/basic-mode/data-sources/json",custom_edit_url:null},sidebar:"docs",previous:{title:"JDBC",permalink:"/Trevas/fr/developer-guide/basic-mode/data-sources/jdbc"},next:{title:"Others",permalink:"/Trevas/fr/developer-guide/basic-mode/data-sources/others"}},i={},d=[{value:"Importer le module jackson de Trevas",id:"importer-le-module-jackson-de-trevas",level:3},{value:"Sch\xe9ma",id:"sch\xe9ma",level:3},{value:"Utilisation du module <code>vtl-jackson</code>",id:"utilisation-du-module-vtl-jackson",level:3},{value:"D\xe9claration globale",id:"d\xe9claration-globale",level:4},{value:"Exemple de d\xe9s\xe9rialisation",id:"exemple-de-d\xe9s\xe9rialisation",level:4}],c={toc:d};function u(e){let{components:t,...r}=e;return(0,a.kt)("wrapper",(0,n.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h3",{id:"importer-le-module-jackson-de-trevas"},"Importer le module jackson de Trevas"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-xml"},"<dependency>\n    <groupId>fr.insee.trevas</groupId>\n    <artifactId>vtl-jackson</artifactId>\n    <version>1.1.1</version>\n</dependency>\n")),(0,a.kt)("h3",{id:"sch\xe9ma"},"Sch\xe9ma"),(0,a.kt)("p",null,"La repr\xe9sentation JSON d'un ",(0,a.kt)("inlineCode",{parentName:"p"},"Dataset")," est d\xe9finie ainsi :"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-json"},'{\n    "dataStructure": [\n        { "name": "id", "type": "STRING", "role": "IDENTIFIER" },\n        { "name": "x", "type": "INTEGER", "role": "MEASURE" },\n        { "name": "y", "type": "FLOAT", "role": "MEASURE" }\n    ],\n    "dataPoints": [\n        ["0001", 10, 50.5],\n        ["0002", 20, -8],\n        ["0003", 1000, 0],\n        ["0004", 1, 4.5]\n    ]\n}\n')),(0,a.kt)("h3",{id:"utilisation-du-module-vtl-jackson"},"Utilisation du module ",(0,a.kt)("inlineCode",{parentName:"h3"},"vtl-jackson")),(0,a.kt)("h4",{id:"d\xe9claration-globale"},"D\xe9claration globale"),(0,a.kt)("p",null,"Le module peut \xeatre d\xe9clar\xe9 globalement \xe0 l'\xe9chelle du projet client."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-java"},"public ObjectMapper objectMapper() {\n    return new ObjectMapper()\n            .registerModule(new TrevasModule());\n}\n")),(0,a.kt)("h4",{id:"exemple-de-d\xe9s\xe9rialisation"},"Exemple de d\xe9s\xe9rialisation"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-java"},"ObjectMapper objectMapper = new ObjectMapper();\nobjectMapper.readValue(json, Dataset.class);\n")))}u.isMDXComponent=!0}}]);