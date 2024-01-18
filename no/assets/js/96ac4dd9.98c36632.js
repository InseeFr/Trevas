"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[7630],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>f});var a=r(67294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var i=a.createContext({}),c=function(e){var t=a.useContext(i),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},u=function(e){var t=c(e.components);return a.createElement(i.Provider,{value:t},e.children)},p="mdxType",b={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},v=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,o=e.originalType,i=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),p=c(r),v=n,f=p["".concat(i,".").concat(v)]||p[v]||b[v]||o;return r?a.createElement(f,l(l({ref:t},u),{},{components:r})):a.createElement(f,l({ref:t},u))}));function f(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=r.length,l=new Array(o);l[0]=v;var s={};for(var i in t)hasOwnProperty.call(t,i)&&(s[i]=t[i]);s.originalType=e,s[p]="string"==typeof e?e:n,l[1]=s;for(var c=2;c<o;c++)l[c]=r[c];return a.createElement.apply(null,l)}return a.createElement.apply(null,r)}v.displayName="MDXCreateElement"},88529:(e,t,r)=>{r.d(t,{Z:()=>n});var a=r(67294);const n=e=>{let{label:t,href:r}=e;return a.createElement("a",{href:r},t)}},95191:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>u,contentTitle:()=>i,default:()=>v,frontMatter:()=>s,metadata:()=>c,toc:()=>p});var a=r(87462),n=(r(67294),r(3905)),o=r(44996),l=r(88529);const s={slug:"/trevas-lab-0.3.3",title:"Trevas Lab 0.3.3",authors:["nicolas"],tags:["Trevas Lab"]},i=void 0,c={permalink:"/Trevas/no/blog/trevas-lab-0.3.3",source:"@site/i18n/no/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-lab-0.3.3.mdx",title:"Trevas Lab 0.3.3",description:"Trevas Lab 0.3.3 uses version 1.0.2 of Trevas.",date:"2023-07-01T00:00:00.000Z",formattedDate:"1. juli 2023",tags:[{label:"Trevas Lab",permalink:"/Trevas/no/blog/tags/trevas-lab"}],readingTime:.335,hasTruncateMarker:!1,authors:[{name:"Nicolas Laval",link:"https://github.com/NicoLaval",title:"Making Sense - Developer",image:"profile_pic_nicolas_laval.jpg",key:"nicolas"}],frontMatter:{slug:"/trevas-lab-0.3.3",title:"Trevas Lab 0.3.3",authors:["nicolas"],tags:["Trevas Lab"]},prevItem:{title:"Trevas Jupyter 0.3.2",permalink:"/Trevas/no/blog/trevas-jupyter-0.3.2"}},u={authorsImageUrls:[void 0]},p=[{value:"News",id:"news",level:3},{value:"Launch",id:"launch",level:3},{value:"Kubernetes",id:"kubernetes",level:4}],b={toc:p};function v(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},b,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("p",null,(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFrLab/Trevas-Lab"},"Trevas Lab")," ",(0,n.kt)("inlineCode",{parentName:"p"},"0.3.3")," uses version ",(0,n.kt)("inlineCode",{parentName:"p"},"1.0.2")," of ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFr/Trevas"},"Trevas"),"."),(0,n.kt)("h3",{id:"news"},"News"),(0,n.kt)("p",null,"In addition to the ",(0,n.kt)(l.Z,{label:"VTL coverage",href:(0,o.Z)("/user-guide/coverage"),mdxType:"Link"})," greatly increased since the publication of Trevas 1.x.x, Trevas Lab offers 2 new connectors:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"SAS files"),(0,n.kt)("li",{parentName:"ul"},"JDBC MariaDB")),(0,n.kt)("h3",{id:"launch"},"Launch"),(0,n.kt)("h4",{id:"kubernetes"},"Kubernetes"),(0,n.kt)("p",null,"Sample Kubernetes objects are available in the ",(0,n.kt)("inlineCode",{parentName:"p"},".kubernetes")," folders of ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFrLab/Trevas-Lab/tree/master/.kubernetes"},"Trevas Lab")," and ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFrLab/Trevas-Lab-UI/tree/master/.kubernetes"},"Trevas Lab UI"),"."))}v.isMDXComponent=!0}}]);