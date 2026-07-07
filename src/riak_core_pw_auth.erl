%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%% Copyright (c) 2026 TI Tokyo.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_core_pw_auth).

-export([hash_password/1, check_password/5]).

%% TOOD should make these configurable in app.config
-define(SALT_LENGTH, 16).
-define(HASH_ITERATIONS, 65536).
%% TODO this should call a default_hash_func() function to get default based on erlang version
-define(HASH_FUNCTION, sha).
-define(KEY_LENGTH, 20).  %% the value used in pbkdf2 for sha
-define(AUTH_NAME, pbkdf2).

%% @doc Hash a plaintext password, returning hashed password and algorithm details
-spec hash_password(binary()) ->
          {ok, binary(), ?AUTH_NAME, ?HASH_FUNCTION, binary(), pos_integer()}.
hash_password(BinaryPass) when is_binary(BinaryPass) ->
    % TODO: Do something more with the salt?
    % Generate salt the simple way
    Salt = crypto:strong_rand_bytes(?SALT_LENGTH),

    % Hash the original password and store as hex
    HashedPass = crypto:pbkdf2_hmac(
                   ?HASH_FUNCTION, BinaryPass, Salt, ?HASH_ITERATIONS, ?KEY_LENGTH),
    HexPass = to_hex(HashedPass),
    {ok, HexPass, ?AUTH_NAME, ?HASH_FUNCTION, Salt, ?HASH_ITERATIONS}.


%% @doc Check a plaintext password with a hashed password
-spec check_password(binary(), binary(), ?HASH_FUNCTION, binary(), pos_integer()) ->
          boolean().
check_password(BinaryPass, HashedPassword, HashFunction, Salt, HashIterations)
  when is_binary(BinaryPass) ->
    % Hash EnteredPassword to compare to HashedPassword
    HashedPass = crypto:pbkdf2_hmac(
                   HashFunction, BinaryPass, Salt, HashIterations, ?KEY_LENGTH),
    HexPass = to_hex(HashedPass),
    compare_secure(binary_to_list(HexPass), binary_to_list(HashedPassword)).


%% copied, slightly simplified, from erlang-pbkdf2/src/pbkdf2.erl
to_hex(Data) ->
    string:lowercase(binary:encode_hex(Data)).

compare_secure(X, Y) ->
    case length(X) == length(Y) of
        true ->
            compare_secure(X, Y, 0);
        false ->
            false
    end.

compare_secure([X|RestX], [Y|RestY], Result) ->
    compare_secure(RestX, RestY, (X bxor Y) bor Result);
compare_secure([], [], Result) ->
    Result == 0.
