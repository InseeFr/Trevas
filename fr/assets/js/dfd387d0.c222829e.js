"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[3279],{3905:(e,r,t)=>{t.d(r,{Zo:()=>l,kt:()=>f});var n=t(67294);function a(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function o(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);r&&(n=n.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,n)}return t}function s(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?o(Object(t),!0).forEach((function(r){a(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function c(e,r){if(null==e)return{};var t,n,a=function(e,r){if(null==e)return{};var t,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)t=o[n],r.indexOf(t)>=0||(a[t]=e[t]);return a}(e,r);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)t=o[n],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var u=n.createContext({}),i=function(e){var r=n.useContext(u),t=r;return e&&(t="function"==typeof e?e(r):s(s({},r),e)),t},l=function(e){var r=i(e.components);return n.createElement(u.Provider,{value:r},e.children)},d="mdxType",p={inlineCode:"code",wrapper:function(e){var r=e.children;return n.createElement(n.Fragment,{},r)}},m=n.forwardRef((function(e,r){var t=e.components,a=e.mdxType,o=e.originalType,u=e.parentName,l=c(e,["components","mdxType","originalType","parentName"]),d=i(t),m=a,f=d["".concat(u,".").concat(m)]||d[m]||p[m]||o;return t?n.createElement(f,s(s({ref:r},l),{},{components:t})):n.createElement(f,s({ref:r},l))}));function f(e,r){var t=arguments,a=r&&r.mdxType;if("string"==typeof e||a){var o=t.length,s=new Array(o);s[0]=m;var c={};for(var u in r)hasOwnProperty.call(r,u)&&(c[u]=r[u]);c.originalType=e,c[d]="string"==typeof e?e:a,s[1]=c;for(var i=2;i<o;i++)s[i]=t[i];return n.createElement.apply(null,s)}return n.createElement.apply(null,t)}m.displayName="MDXCreateElement"},54036:(e,r,t)=>{t.r(r),t.d(r,{assets:()=>u,contentTitle:()=>s,default:()=>d,frontMatter:()=>o,metadata:()=>c,toc:()=>i});var n=t(87462),a=(t(67294),t(3905));const o={id:"others",title:"Mode de base - Autres sources",sidebar_label:"Others",slug:"/developer-guide/basic-mode/data-sources/others",custom_edit_url:null},s=void 0,c={unversionedId:"developer-guide/basic-mode/data-sources/others",id:"developer-guide/basic-mode/data-sources/others",title:"Mode de base - Autres sources",description:"Constructeur InMemoryDataset",source:"@site/i18n/fr/docusaurus-plugin-content-docs/current/developer-guide/basic-mode/data-sources/others.mdx",sourceDirName:"developer-guide/basic-mode/data-sources",slug:"/developer-guide/basic-mode/data-sources/others",permalink:"/Trevas/fr/developer-guide/basic-mode/data-sources/others",draft:!1,editUrl:null,tags:[],version:"current",lastUpdatedAt:1705601833,formattedLastUpdatedAt:"18 janv. 2024",frontMatter:{id:"others",title:"Mode de base - Autres sources",sidebar_label:"Others",slug:"/developer-guide/basic-mode/data-sources/others",custom_edit_url:null},sidebar:"docs",previous:{title:"JSON",permalink:"/Trevas/fr/developer-guide/basic-mode/data-sources/json"},next:{title:"Vue d'ensemble",permalink:"/Trevas/fr/developer-guide/spark-mode"}},u={},i=[{value:"Constructeur <code>InMemoryDataset</code>",id:"constructeur-inmemorydataset",level:3}],l={toc:i};function d(e){let{components:r,...t}=e;return(0,a.kt)("wrapper",(0,n.Z)({},l,t,{components:r,mdxType:"MDXLayout"}),(0,a.kt)("h3",{id:"constructeur-inmemorydataset"},"Constructeur ",(0,a.kt)("inlineCode",{parentName:"h3"},"InMemoryDataset")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-java"},'InMemoryDataset dataset = new InMemoryDataset(\n                List.of(\n                        Map.of("var1", "x", "var2", "y", "var3", 5),\n                        Map.of("var1", "xx", "var2", "yy", "var3", 10)\n                ),\n                Map.of("var1", String.class, "var2", String.class, "var3", Long.class),\n                Map.of("var1", Role.IDENTIFIER, "var2", Role.ATTRIBUTE, "var3", Role.MEASURE)\n);\n')))}d.isMDXComponent=!0}}]);