"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[5715],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>b});var n=r(7294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function c(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function d(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var i=n.createContext({}),l=function(e){var t=n.useContext(i),r=t;return e&&(r="function"==typeof e?e(t):c(c({},t),e)),r},u=function(e){var t=l(e.components);return n.createElement(i.Provider,{value:t},e.children)},s="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,a=e.originalType,i=e.parentName,u=d(e,["components","mdxType","originalType","parentName"]),s=l(r),m=o,b=s["".concat(i,".").concat(m)]||s[m]||p[m]||a;return r?n.createElement(b,c(c({ref:t},u),{},{components:r})):n.createElement(b,c({ref:t},u))}));function b(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=r.length,c=new Array(a);c[0]=m;var d={};for(var i in t)hasOwnProperty.call(t,i)&&(d[i]=t[i]);d.originalType=e,d[s]="string"==typeof e?e:o,c[1]=d;for(var l=2;l<a;l++)c[l]=r[l];return n.createElement.apply(null,c)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},4725:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>i,contentTitle:()=>c,default:()=>s,frontMatter:()=>a,metadata:()=>d,toc:()=>l});var n=r(7462),o=(r(7294),r(3905));const a={id:"jdbc",title:"Grunnleggende modus - JDBC-kilde",sidebar_label:"JDBC",slug:"/developer-guide/basic-mode/data-sources/jdbc",custom_edit_url:null},c=void 0,d={unversionedId:"developer-guide/basic-mode/data-sources/jdbc",id:"developer-guide/basic-mode/data-sources/jdbc",title:"Grunnleggende modus - JDBC-kilde",description:"Importer JDBC-modul fra Trevas",source:"@site/i18n/no/docusaurus-plugin-content-docs/current/developer-guide/basic-mode/data-sources/jdbc.mdx",sourceDirName:"developer-guide/basic-mode/data-sources",slug:"/developer-guide/basic-mode/data-sources/jdbc",permalink:"/Trevas/no/developer-guide/basic-mode/data-sources/jdbc",draft:!1,editUrl:null,tags:[],version:"current",lastUpdatedAt:1693210358,formattedLastUpdatedAt:"28. aug. 2023",frontMatter:{id:"jdbc",title:"Grunnleggende modus - JDBC-kilde",sidebar_label:"JDBC",slug:"/developer-guide/basic-mode/data-sources/jdbc",custom_edit_url:null},sidebar:"docs",previous:{title:"Oversikt",permalink:"/Trevas/no/developer-guide/basic-mode/data-sources"},next:{title:"JSON",permalink:"/Trevas/no/developer-guide/basic-mode/data-sources/json"}},i={},l=[{value:"Importer JDBC-modul fra Trevas",id:"importer-jdbc-modul-fra-trevas",level:3},{value:"Bruke <code>vtl-jdbc</code> modulen",id:"bruke-vtl-jdbc-modulen",level:3}],u={toc:l};function s(e){let{components:t,...r}=e;return(0,o.kt)("wrapper",(0,n.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h3",{id:"importer-jdbc-modul-fra-trevas"},"Importer JDBC-modul fra Trevas"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-xml"},"<dependency>\n    <groupId>fr.insee.trevas</groupId>\n    <artifactId>vtl-jdbc</artifactId>\n    <version>1.0.2</version>\n</dependency>\n")),(0,o.kt)("h3",{id:"bruke-vtl-jdbc-modulen"},"Bruke ",(0,o.kt)("inlineCode",{parentName:"h3"},"vtl-jdbc")," modulen"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-java"},'connection = DriverManager.getConnection(...);\n\nStatement statement = connection.createStatement();\n\nJDBCDataset jdbcDataset = new JDBCDataset(() -> {\n    try {\n        return statement.executeQuery("select * from ds1;");\n    } catch (SQLException se) {\n        throw new RuntimeException(se);\n    }\n});\n')))}s.isMDXComponent=!0}}]);