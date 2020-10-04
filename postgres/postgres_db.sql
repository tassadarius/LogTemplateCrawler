--
-- PostgreSQL database dump
--

-- Dumped from database version 12.3
-- Dumped by pg_dump version 12.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: rotate(); Type: FUNCTION; Schema: public; Owner: tassadarius
--

CREATE FUNCTION public.rotate() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM cursor WHERE date NOT IN (SELECT date FROM cursor ORDER BY date DESC LIMIT 10); RETURN NULL;
END;
$$;


ALTER FUNCTION public.rotate() OWNER TO tassadarius;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: cursor; Type: TABLE; Schema: public; Owner: tassadarius
--

CREATE TABLE public.cursor (
    cursor integer,
    date timestamp without time zone DEFAULT now()
);


ALTER TABLE public.cursor OWNER TO tassadarius;

--
-- Name: discarded_repositories_repo_id_seq; Type: SEQUENCE; Schema: public; Owner: tassadarius
--

CREATE SEQUENCE public.discarded_repositories_repo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.discarded_repositories_repo_id_seq OWNER TO tassadarius;

--
-- Name: discarded_repositories; Type: TABLE; Schema: public; Owner: tassadarius
--

CREATE TABLE public.discarded_repositories (
    repo_id integer DEFAULT nextval('public.discarded_repositories_repo_id_seq'::regclass) NOT NULL,
    name character varying(96),
    url character varying(255) NOT NULL,
    contains_logging boolean,
    processed boolean,
    is_fork boolean,
    stars integer,
    cursor integer,
    disk_usage integer,
    license text,
    languages text[],
    owner character varying(96)
);


ALTER TABLE public.discarded_repositories OWNER TO tassadarius;

--
-- Name: discarded_templates; Type: TABLE; Schema: public; Owner: tassadarius
--

CREATE TABLE public.discarded_templates (
    template_id integer,
    repo_id integer,
    template text,
    crawl_date timestamp with time zone,
    framework character varying(16),
    raw text,
    parsed_template text,
    arguments text[],
    disabled boolean
);


ALTER TABLE public.discarded_templates OWNER TO tassadarius;

--
-- Name: repositories; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.repositories (
    repo_id integer NOT NULL,
    name character varying(96),
    url character varying(255) NOT NULL,
    contains_logging boolean DEFAULT false,
    processed boolean DEFAULT false,
    is_fork boolean DEFAULT false,
    stars integer,
    cursor integer,
    disk_usage integer,
    license text,
    languages text[],
    owner character varying(96),
    framework character varying(24),
    successfully_processed boolean,
    locked boolean DEFAULT false,
    main_language character varying(40)
);


ALTER TABLE public.repositories OWNER TO postgres;

--
-- Name: repositories_repo_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.repositories_repo_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.repositories_repo_id_seq OWNER TO postgres;

--
-- Name: repositories_repo_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.repositories_repo_id_seq OWNED BY public.repositories.repo_id;


--
-- Name: templates; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.templates (
    template_id integer NOT NULL,
    repo_id integer NOT NULL,
    template text NOT NULL,
    crawl_date timestamp with time zone,
    framework character varying(16),
    raw text,
    parsed_template text,
    arguments text[],
    disabled boolean
);


ALTER TABLE public.templates OWNER TO postgres;

--
-- Name: templates_template_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.templates_template_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.templates_template_id_seq OWNER TO postgres;

--
-- Name: templates_template_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.templates_template_id_seq OWNED BY public.templates.template_id;


--
-- Name: repositories repo_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.repositories ALTER COLUMN repo_id SET DEFAULT nextval('public.repositories_repo_id_seq'::regclass);


--
-- Name: templates template_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.templates ALTER COLUMN template_id SET DEFAULT nextval('public.templates_template_id_seq'::regclass);


--
-- Name: discarded_repositories discarded_repositories_pkey; Type: CONSTRAINT; Schema: public; Owner: tassadarius
--

ALTER TABLE ONLY public.discarded_repositories
    ADD CONSTRAINT discarded_repositories_pkey PRIMARY KEY (repo_id);


--
-- Name: discarded_repositories discarded_repositories_url_key; Type: CONSTRAINT; Schema: public; Owner: tassadarius
--

ALTER TABLE ONLY public.discarded_repositories
    ADD CONSTRAINT discarded_repositories_url_key UNIQUE (url);


--
-- Name: discarded_templates discarded_templates_parsed_template_key; Type: CONSTRAINT; Schema: public; Owner: tassadarius
--

ALTER TABLE ONLY public.discarded_templates
    ADD CONSTRAINT discarded_templates_parsed_template_key UNIQUE (parsed_template);


--
-- Name: templates no_duplicates; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.templates
    ADD CONSTRAINT no_duplicates UNIQUE (template);


--
-- Name: repositories repositories_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.repositories
    ADD CONSTRAINT repositories_pkey PRIMARY KEY (repo_id);


--
-- Name: repositories repositories_url_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.repositories
    ADD CONSTRAINT repositories_url_key UNIQUE (url);


--
-- Name: templates templates_parsed_template_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.templates
    ADD CONSTRAINT templates_parsed_template_key UNIQUE (parsed_template);


--
-- Name: templates templates_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.templates
    ADD CONSTRAINT templates_pkey PRIMARY KEY (template_id);


--
-- Name: url_idx; Type: INDEX; Schema: public; Owner: tassadarius
--

CREATE UNIQUE INDEX url_idx ON public.discarded_repositories USING btree (url);


--
-- Name: cursor rotate_cursors; Type: TRIGGER; Schema: public; Owner: tassadarius
--

CREATE TRIGGER rotate_cursors AFTER INSERT ON public.cursor FOR EACH STATEMENT EXECUTE FUNCTION public.rotate();


--
-- Name: templates templates_repository_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.templates
    ADD CONSTRAINT templates_repository_fkey FOREIGN KEY (repo_id) REFERENCES public.repositories(repo_id);


--
-- PostgreSQL database dump complete
--

