"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[8893],{39308:(e,t,r)=>{r.d(t,{Z:()=>v});var n=r(67294),i=r(3905),a=r(84734);const d="riContainer_QAfm",l="riDescriptionShort_ogAL",o="riDetail_rDiW",s="riIcon_zSrR",c="riTitle__Mkd",u="riDescription_k_lG",p="riMore_lLbd";var m=r(86010);const v=e=>{const[t,r]=n.useState(!1);return n.createElement("a",{href:e.page,className:d},n.createElement("div",{className:l},n.createElement("div",{className:s},n.createElement("span",{className:"fe fe-zap"})),n.createElement("div",{className:o},n.createElement("div",{className:c},n.createElement("a",{href:e.page},e.title)),n.createElement("div",{className:u},e.description,n.Children.count(e.children)>0&&n.createElement("span",{className:(0,m.Z)(p,"fe","fe-more-horizontal"),onClick:()=>r(!t)})))),t&&n.createElement("div",{className:"ri-description-long"},n.createElement(i.Zo,{components:a.Z},e.children)))}},26739:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>c,contentTitle:()=>o,default:()=>m,frontMatter:()=>l,metadata:()=>s,toc:()=>u});var n=r(87462),i=(r(67294),r(3905)),a=r(44996),d=r(39308);const l={id:"index-developer-guide",title:"Guide d\xe9veloppeur",sidebar_label:"Vue d'ensemble",slug:"/developer-guide",custom_edit_url:null},o=void 0,s={unversionedId:"developer-guide/index-developer-guide",id:"developer-guide/index-developer-guide",title:"Guide d\xe9veloppeur",description:"Importer le moteur Trevas",source:"@site/i18n/fr/docusaurus-plugin-content-docs/current/developer-guide/index-developer-guide.mdx",sourceDirName:"developer-guide",slug:"/developer-guide",permalink:"/Trevas/fr/developer-guide",draft:!1,editUrl:null,tags:[],version:"current",lastUpdatedAt:1705601833,formattedLastUpdatedAt:"18 janv. 2024",frontMatter:{id:"index-developer-guide",title:"Guide d\xe9veloppeur",sidebar_label:"Vue d'ensemble",slug:"/developer-guide",custom_edit_url:null},sidebar:"docs",previous:{title:"Op\xe9rateurs de clause",permalink:"/Trevas/fr/user-guide/coverage/clause-operators"},next:{title:"Vue d'ensemble",permalink:"/Trevas/fr/developer-guide/basic-mode"}},c={},u=[{value:"Importer le moteur Trevas",id:"importer-le-moteur-trevas",level:3},{value:"Instancier le moteur Trevas",id:"instancier-le-moteur-trevas",level:3},{value:"Mode d&#39;ex\xe9cution",id:"mode-dex\xe9cution",level:3}],p={toc:u};function m(e){let{components:t,...r}=e;return(0,i.kt)("wrapper",(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h3",{id:"importer-le-moteur-trevas"},"Importer le moteur Trevas"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-xml"},"<dependency>\n    <groupId>fr.insee.trevas</groupId>\n    <artifactId>vtl-engine</artifactId>\n    <version>1.3.0</version>\n</dependency>\n")),(0,i.kt)("h3",{id:"instancier-le-moteur-trevas"},"Instancier le moteur Trevas"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'// Start engine\nScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");\n\n// Add input bindings to engine\nBindings bindings = new SimpleBindings();\nbindings.put("a", 1);\nengine.setBindings(b, ScriptContext.ENGINE_SCOPE);\n\n// Execute script\ntry {\n        engine.eval("b := a + 1;");\n} catch (VtlScriptException e) {\n        logger.warn("Eval failed: ", e);\n}\n\n// Get result\nLong result = (Long) engine.getBindings(ScriptContext.ENGINE_SCOPE).get("b");\n')),(0,i.kt)("h3",{id:"mode-dex\xe9cution"},"Mode d'ex\xe9cution"),(0,i.kt)("div",{className:"row"},(0,i.kt)("div",{className:"col"},(0,i.kt)(d.Z,{title:"Basic mode",page:(0,a.Z)("/developer-guide/basic-mode"),mdxType:"Card"})),(0,i.kt)("div",{className:"col"},(0,i.kt)(d.Z,{title:"Spark mode",page:(0,a.Z)("/developer-guide/spark-mode"),mdxType:"Card"}))))}m.isMDXComponent=!0}}]);