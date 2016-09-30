--
-- PostgreSQL database dump
--

-- Dumped from database version 9.1.9
-- Dumped by pg_dump version 9.1.9

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: tb_sandbox_child_result; Type: TABLE; Schema: public; Owner: root
--

CREATE TABLE tb_sandbox_child_result (
    CONSTRAINT tb_sandbox_child_result_parentsha1_check CHECK (((parentsha1 <> ''::bpchar) AND (parentsha1 IS NOT NULL)))
)
INHERITS (tb_sandbox_result);


ALTER TABLE tb_sandbox_child_result OWNER TO root;

--
-- PostgreSQL database dump complete
--

